package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/SomethingCreativeStudios/hronir/internal/catalog"
	"github.com/SomethingCreativeStudios/hronir/internal/config"
	"github.com/SomethingCreativeStudios/hronir/internal/events"
	"github.com/SomethingCreativeStudios/hronir/internal/harvest"
	"github.com/SomethingCreativeStudios/hronir/internal/mapping"
	"github.com/SomethingCreativeStudios/hronir/internal/metrics"
	"github.com/SomethingCreativeStudios/hronir/internal/model"
	"github.com/SomethingCreativeStudios/hronir/internal/sink"
	"github.com/SomethingCreativeStudios/hronir/internal/source"
	"github.com/SomethingCreativeStudios/hronir/internal/state"
	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	root := newRoot()
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "hronir:", err)
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	var configPath string
	root := &cobra.Command{Use: "hronir", Short: "Materialize Connected Systems metadata into OGC API Records", SilenceUsage: true}
	root.PersistentFlags().StringVarP(&configPath, "config", "c", "hronir.yaml", "Hronir configuration file")
	load := func() (config.Config, error) { return config.Load(configPath) }
	root.AddCommand(&cobra.Command{Use: "version", Short: "Print the Hronir version", Run: func(*cobra.Command, []string) { fmt.Println(version) }})
	root.AddCommand(configCommand(load))
	root.AddCommand(catalogCommand(load))
	root.AddCommand(syncCommand(load))
	root.AddCommand(runCommand(load))
	root.AddCommand(mappingCommand(load))
	root.AddCommand(remapCommand(load))
	root.AddCommand(failureCommand(load))
	root.AddCommand(pruneCommand(load))
	root.AddCommand(doctorCommand(load))
	root.AddCommand(&cobra.Command{Use: "healthcheck", Short: "Confirm that the executable is available", Run: func(*cobra.Command, []string) { fmt.Println("ok") }})
	return root
}

func configCommand(load func() (config.Config, error)) *cobra.Command {
	command := &cobra.Command{Use: "config", Short: "Configuration commands", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error { return cmd.Help() }}
	command.AddCommand(&cobra.Command{Use: "validate", Short: "Validate configuration and resolve required secrets", RunE: func(cmd *cobra.Command, args []string) error {
		_, err := load()
		if err == nil {
			fmt.Fprintln(cmd.OutOrStdout(), "ok")
		}
		return err
	}})
	return command
}
func catalogCommand(load func() (config.Config, error)) *cobra.Command {
	command := &cobra.Command{Use: "catalog", Short: "Catalog bundle commands"}
	var format string
	render := &cobra.Command{Use: "render", Short: "Render the target catalog bundle", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := load()
		if err != nil {
			return err
		}
		if format != "tlon" {
			return fmt.Errorf("unsupported catalog format %q", format)
		}
		data, err := catalog.RenderTlon(cfg)
		if err == nil {
			_, err = cmd.OutOrStdout().Write(append(data, '\n'))
		}
		return err
	}}
	render.Flags().StringVar(&format, "format", "tlon", "Catalog format")
	command.AddCommand(render)
	return command
}
func syncCommand(load func() (config.Config, error)) *cobra.Command {
	var sourceID, kind string
	command := &cobra.Command{Use: "sync", Short: "Run a single source synchronization", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := load()
		if err != nil {
			return err
		}
		runtime, err := openRuntime(cmd.Context(), cfg)
		if err != nil {
			return err
		}
		defer runtime.close()
		if err := runtime.coordinator.CheckTarget(cmd.Context()); err != nil {
			return err
		}
		return runtime.coordinator.Sync(cmd.Context(), sourceID, kind)
	}}
	command.Flags().StringVar(&sourceID, "source", "", "Only synchronize this source")
	command.Flags().StringVar(&kind, "kind", "", "Only synchronize this resource kind")
	return command
}
func remapCommand(load func() (config.Config, error)) *cobra.Command {
	return &cobra.Command{Use: "remap", Short: "Reapply the mapping to cached normalized resources", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := load()
		if err != nil {
			return err
		}
		runtime, err := openRuntime(cmd.Context(), cfg)
		if err != nil {
			return err
		}
		defer runtime.close()
		if err := runtime.coordinator.CheckTarget(cmd.Context()); err != nil {
			return err
		}
		if err := runtime.coordinator.RetryFailures(cmd.Context()); err != nil {
			return err
		}
		return runtime.coordinator.ReconcileTarget(cmd.Context())
	}}
}
func mappingCommand(load func() (config.Config, error)) *cobra.Command {
	command := &cobra.Command{Use: "mapping", Short: "Mapping commands"}
	var kind, input string
	test := &cobra.Command{Use: "test", Short: "Apply the active mapping to canonical resource JSON", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := load()
		if err != nil {
			return err
		}
		if kind == "" || input == "" {
			return errors.New("--kind and --input are required")
		}
		data, err := os.ReadFile(input)
		if err != nil {
			return err
		}
		var resource model.Resource
		if err := json.Unmarshal(data, &resource); err != nil {
			return err
		}
		resource.Kind = kind
		engine, err := mapping.New(cfg.Mapping)
		if err != nil {
			return err
		}
		record, err := engine.Map(resource, "mapping-test", "https://mapping.test", time.Now())
		if err != nil {
			return err
		}
		data, err = json.MarshalIndent(record, "", "  ")
		if err == nil {
			_, err = cmd.OutOrStdout().Write(append(data, '\n'))
		}
		return err
	}}
	test.Flags().StringVar(&kind, "kind", "", "Connected Systems kind")
	test.Flags().StringVar(&input, "input", "", "Canonical resource JSON file")
	command.AddCommand(test)
	return command
}
func failureCommand(load func() (config.Config, error)) *cobra.Command {
	command := &cobra.Command{Use: "failures", Short: "Inspect and retry durable failures"}
	command.AddCommand(&cobra.Command{Use: "list", Short: "List failures", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := load()
		if err != nil {
			return err
		}
		store, err := state.Open(cfg.State.Path)
		if err != nil {
			return err
		}
		defer store.Close()
		items, err := store.Failures(cmd.Context())
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(items)
	}})
	command.AddCommand(&cobra.Command{Use: "retry", Short: "Retry cached mapping and sink failures", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := load()
		if err != nil {
			return err
		}
		runtime, err := openRuntime(cmd.Context(), cfg)
		if err != nil {
			return err
		}
		defer runtime.close()
		return runtime.coordinator.RetryFailures(cmd.Context())
	}})
	return command
}
func pruneCommand(load func() (config.Config, error)) *cobra.Command {
	var sourceID, kind string
	var yes bool
	command := &cobra.Command{Use: "prune", Short: "Explicitly delete managed target records", RunE: func(cmd *cobra.Command, args []string) error {
		if sourceID == "" || kind == "" || !yes {
			return errors.New("--source, --kind, and --yes are required")
		}
		cfg, err := load()
		if err != nil {
			return err
		}
		runtime, err := openRuntime(cmd.Context(), cfg)
		if err != nil {
			return err
		}
		defer runtime.close()
		if err := runtime.coordinator.CheckTarget(cmd.Context()); err != nil {
			return err
		}
		count, err := runtime.coordinator.Prune(cmd.Context(), sourceID, kind)
		if err == nil {
			fmt.Fprintf(cmd.OutOrStdout(), "pruned %d records\n", count)
		}
		return err
	}}
	command.Flags().StringVar(&sourceID, "source", "", "Source ID")
	command.Flags().StringVar(&kind, "kind", "", "Resource kind")
	command.Flags().BoolVar(&yes, "yes", false, "Confirm destructive operation")
	return command
}
func doctorCommand(load func() (config.Config, error)) *cobra.Command {
	return &cobra.Command{Use: "doctor", Short: "Check target and source connectivity", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := load()
		if err != nil {
			return err
		}
		runtime, err := openRuntime(cmd.Context(), cfg)
		if err != nil {
			return err
		}
		defer runtime.close()
		if err := runtime.coordinator.CheckTarget(cmd.Context()); err != nil {
			return err
		}
		for _, sourceConfig := range cfg.Sources {
			client, err := source.New(cmd.Context(), sourceConfig, cfg.Runtime.RequestTimeout)
			if err != nil {
				return fmt.Errorf("source %s: %w", sourceConfig.ID, err)
			}
			if err := client.Discover(cmd.Context()); err != nil {
				return fmt.Errorf("source %s: %w", sourceConfig.ID, err)
			}
		}
		fmt.Fprintln(cmd.OutOrStdout(), "ok")
		return nil
	}}
}

func runCommand(load func() (config.Config, error)) *cobra.Command {
	return &cobra.Command{Use: "run", Short: "Run the daemon, metrics server, and MQTT listeners", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := load()
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		runtime, err := openRuntime(ctx, cfg)
		if err != nil {
			return err
		}
		defer runtime.close()
		var ready atomic.Bool
		mux := http.NewServeMux()
		mux.Handle("/metrics", runtime.metrics.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
		})
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
			if !ready.Load() {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte("ready\n"))
		})
		server := &http.Server{Addr: cfg.Runtime.ListenAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		serverErrors := make(chan error, 1)
		go func() {
			err := server.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				serverErrors <- err
			}
		}()
		subscriptions, err := subscribe(ctx, cfg, runtime.coordinator)
		if err != nil {
			_ = server.Shutdown(context.Background())
			return err
		}
		defer func() {
			for _, subscription := range subscriptions {
				subscription.Close()
			}
		}()
		runErrors := make(chan error, 1)
		go func() { runErrors <- runtime.coordinator.Run(ctx, func() { ready.Store(true) }) }()
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
			err := <-runErrors
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		case err := <-serverErrors:
			return err
		case err := <-runErrors:
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}}
}

type runtime struct {
	store       *state.Store
	coordinator *harvest.Coordinator
	metrics     *metrics.Metrics
}

func openRuntime(ctx context.Context, cfg config.Config) (*runtime, error) {
	store, err := state.Open(cfg.State.Path)
	if err != nil {
		return nil, err
	}
	engine, err := mapping.New(cfg.Mapping)
	if err != nil {
		store.Close()
		return nil, err
	}
	target, err := sink.NewHTTP(ctx, cfg.Target, cfg.Runtime.RequestTimeout)
	if err != nil {
		store.Close()
		return nil, err
	}
	metricSet := metrics.New()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return &runtime{store: store, metrics: metricSet, coordinator: harvest.New(cfg, store, engine, target, logger, metricSet)}, nil
}
func (r *runtime) close() { _ = r.store.Close() }

func subscribe(ctx context.Context, cfg config.Config, coordinator *harvest.Coordinator) ([]*events.Subscription, error) {
	var result []*events.Subscription
	for _, sourceConfig := range cfg.Sources {
		if sourceConfig.MQTT.Enabled != nil && !*sourceConfig.MQTT.Enabled {
			continue
		}
		broker, topics := sourceConfig.MQTT.Broker, sourceConfig.MQTT.Topics
		if broker == "" || len(topics) == 0 {
			discoveredBroker, discoveredTopics, err := events.Discover(ctx, sourceConfig, cfg.Runtime.RequestTimeout)
			if err != nil {
				if sourceConfig.MQTT.Required {
					return result, fmt.Errorf("discover MQTT for %s: %w", sourceConfig.ID, err)
				}
				slog.Warn("MQTT unavailable; polling remains authoritative", "source", sourceConfig.ID, "error", err)
				continue
			}
			if broker == "" {
				broker = discoveredBroker
			}
			if len(topics) == 0 {
				topics = discoveredTopics
			}
		}
		sourceID := sourceConfig.ID
		subscription, err := events.Subscribe(ctx, sourceConfig, broker, topics, func(payload []byte) {
			go func() {
				if err := coordinator.HandleCloudEvent(context.Background(), sourceID, payload); err != nil && !strings.Contains(err.Error(), "ignore unsupported") {
					slog.Warn("MQTT event reconciliation failed", "source", sourceID, "error", err)
				}
			}()
		})
		if err != nil {
			if sourceConfig.MQTT.Required {
				return result, fmt.Errorf("subscribe MQTT for %s: %w", sourceID, err)
			}
			slog.Warn("MQTT unavailable; polling remains authoritative", "source", sourceID, "error", err)
			continue
		}
		result = append(result, subscription)
	}
	return result, nil
}
