// Package mapping compiles Hronir's embedded profile and safe local overlays.
package mapping

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SomethingCreativeStudios/hronir/internal/config"
	"github.com/SomethingCreativeStudios/hronir/internal/model"
	"github.com/dop251/goja"
	"github.com/theory/jsonpath"
)

type Engine struct {
	profileDigest string
	fields        map[string]map[string]field
	scripts       []script
	pool          sync.Pool
}

type field struct {
	Expr    string
	Literal any
	Remove  bool
}
type script struct {
	name    string
	source  string
	program *goja.Program
}
type missing struct{}

// New compiles the built-in profile and all configured overlays and scripts.
func New(cfg config.MappingConfig) (*Engine, error) {
	engine := &Engine{fields: builtInFields()}
	for kind, overlay := range cfg.Overlays {
		if engine.fields[kind] == nil {
			engine.fields[kind] = map[string]field{}
		}
		for pointer, override := range overlay.Fields {
			if override.Remove {
				engine.fields[kind][pointer] = field{Remove: true}
				continue
			}
			engine.fields[kind][pointer] = field{Expr: override.Expr, Literal: override.Literal}
		}
	}
	for _, filename := range cfg.Functions {
		source, err := os.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("read function %s: %w", filename, err)
		}
		program, err := goja.Compile(filename, string(source), false)
		if err != nil {
			return nil, fmt.Errorf("compile function %s: %w", filename, err)
		}
		engine.scripts = append(engine.scripts, script{name: filename, source: string(source), program: program})
	}
	digest, err := model.Hash(map[string]any{"fields": engine.serializableFields(), "functions": engine.functionSources()})
	if err != nil {
		return nil, err
	}
	engine.profileDigest = digest
	engine.pool.New = func() any {
		holder, err := engine.newRuntime()
		if err != nil {
			return err
		}
		return holder
	}
	return engine, nil
}

func (e *Engine) ProfileDigest() string { return e.profileDigest }

func (e *Engine) Map(resource model.Resource, sourceID, sourceURL string, now time.Time) (model.Record, error) {
	record, err := model.NewRecord(resource, sourceID, sourceURL, e.profileDigest, now)
	if err != nil {
		return nil, err
	}
	input, err := toMap(resource)
	if err != nil {
		return nil, err
	}
	fields := e.fields[resource.Kind]
	pointers := make([]string, 0, len(fields))
	for pointer := range fields {
		pointers = append(pointers, pointer)
	}
	sort.Strings(pointers)
	for _, pointer := range pointers {
		item := fields[pointer]
		if item.Remove {
			deletePointer(record, pointer)
			continue
		}
		value := item.Literal
		if item.Expr != "" {
			value, err = e.evaluate(input, item.Expr)
			if err != nil {
				return nil, fmt.Errorf("%s %s: %w", resource.Kind, pointer, err)
			}
		}
		if _, absent := value.(missing); absent {
			deletePointer(record, pointer)
			continue
		}
		if err := setPointer(record, pointer, value); err != nil {
			return nil, err
		}
	}
	return record, nil
}

func builtInFields() map[string]map[string]field {
	result := map[string]map[string]field{}
	for _, kind := range config.ResourceKinds {
		result[kind] = map[string]field{
			"/properties/title":                         {Expr: "= $.name | trim | default($.uid) | default($.id)"},
			"/properties/description":                   {Expr: "= $.description | trim"},
			"/properties/keywords":                      {Expr: "= $.keywords | compact | unique"},
			"/geometry":                                 {Expr: "= $.geometry"},
			"/properties/connectedSystems/identifiers":  {Expr: "= $.identifiers"},
			"/properties/connectedSystems/classifiers":  {Expr: "= $.classifiers"},
			"/properties/connectedSystems/contacts":     {Expr: "= $.contacts"},
			"/properties/connectedSystems/associations": {Expr: "= $.associations"},
			"/properties/connectedSystems/metadata":     {Expr: "= $.metadata"},
			"/properties/connectedSystems/extensions":   {Expr: "= $.extensions"},
		}
	}
	return result
}

func (e *Engine) evaluate(input map[string]any, expression string) (any, error) {
	steps, err := splitPipeline(strings.TrimSpace(strings.TrimPrefix(expression, "=")))
	if err != nil {
		return nil, err
	}
	if len(steps) == 0 {
		return nil, errors.New("empty expression")
	}
	value, err := selectPath(input, steps[0])
	if err != nil {
		return nil, err
	}
	for _, step := range steps[1:] {
		name, arguments, err := parseCall(step)
		if err != nil {
			return nil, err
		}
		resolved := make([]any, len(arguments))
		for i, argument := range arguments {
			resolved[i], err = resolveArgument(input, argument)
			if err != nil {
				return nil, err
			}
		}
		value, err = e.call(name, value, resolved)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

func selectPath(input map[string]any, expression string) (any, error) {
	path, err := jsonpath.Parse(strings.TrimSpace(expression))
	if err != nil {
		return nil, fmt.Errorf("parse JSONPath %q: %w", expression, err)
	}
	values := path.Select(input)
	if len(values) == 0 {
		return missing{}, nil
	}
	if len(values) == 1 {
		return values[0], nil
	}
	return []any(values), nil
}

func resolveArgument(input map[string]any, argument string) (any, error) {
	argument = strings.TrimSpace(argument)
	if strings.HasPrefix(argument, "$") {
		return selectPath(input, argument)
	}
	if len(argument) >= 2 && ((argument[0] == '"' && argument[len(argument)-1] == '"') || (argument[0] == '\'' && argument[len(argument)-1] == '\'')) {
		return strconv.Unquote("\"" + strings.ReplaceAll(argument[1:len(argument)-1], "\"", "\\\"") + "\"")
	}
	if argument == "null" {
		return nil, nil
	}
	if argument == "true" {
		return true, nil
	}
	if argument == "false" {
		return false, nil
	}
	if number, err := strconv.ParseFloat(argument, 64); err == nil {
		return number, nil
	}
	return nil, fmt.Errorf("unsupported function argument %q", argument)
}

func (e *Engine) call(name string, value any, arguments []any) (any, error) {
	switch name {
	case "trim":
		return vector(value, func(v any) (any, error) {
			if text, ok := v.(string); ok {
				return strings.TrimSpace(text), nil
			}
			return v, nil
		})
	case "lower":
		return vector(value, func(v any) (any, error) {
			if text, ok := v.(string); ok {
				return strings.ToLower(text), nil
			}
			return v, nil
		})
	case "upper":
		return vector(value, func(v any) (any, error) {
			if text, ok := v.(string); ok {
				return strings.ToUpper(text), nil
			}
			return v, nil
		})
	case "compact":
		return compact(value), nil
	case "unique":
		return unique(value), nil
	case "first":
		if values, ok := value.([]any); ok {
			if len(values) == 0 {
				return missing{}, nil
			}
			return values[0], nil
		}
		return value, nil
	case "flatten":
		return flatten(value), nil
	case "default", "coalesce":
		if isEmpty(value) {
			for _, item := range arguments {
				if !isEmpty(item) {
					return item, nil
				}
			}
			return value, nil
		}
		return value, nil
	case "split":
		if len(arguments) != 1 {
			return nil, errors.New("split requires one separator")
		}
		sep, ok := arguments[0].(string)
		if !ok {
			return nil, errors.New("split separator must be string")
		}
		return vector(value, func(v any) (any, error) {
			if text, ok := v.(string); ok {
				return strings.Split(text, sep), nil
			}
			return v, nil
		})
	case "join":
		if len(arguments) != 1 {
			return nil, errors.New("join requires one separator")
		}
		sep, ok := arguments[0].(string)
		if !ok {
			return nil, errors.New("join separator must be string")
		}
		if values, ok := value.([]any); ok {
			parts := make([]string, 0, len(values))
			for _, v := range values {
				if text, ok := v.(string); ok {
					parts = append(parts, text)
				}
			}
			return strings.Join(parts, sep), nil
		}
		return value, nil
	case "replace":
		if len(arguments) != 2 {
			return nil, errors.New("replace requires old and new values")
		}
		old, ok1 := arguments[0].(string)
		new, ok2 := arguments[1].(string)
		if !ok1 || !ok2 {
			return nil, errors.New("replace arguments must be strings")
		}
		return vector(value, func(v any) (any, error) {
			if text, ok := v.(string); ok {
				return strings.ReplaceAll(text, old, new), nil
			}
			return v, nil
		})
	case "truncate":
		if len(arguments) != 1 {
			return nil, errors.New("truncate requires length")
		}
		limit, ok := arguments[0].(float64)
		if !ok {
			return nil, errors.New("truncate length must be numeric")
		}
		return vector(value, func(v any) (any, error) {
			if text, ok := v.(string); ok && len(text) > int(limit) {
				return text[:int(limit)], nil
			}
			return v, nil
		})
	default:
		if !strings.Contains(name, ".") {
			return nil, fmt.Errorf("unknown mapping function %q", name)
		}
		return e.callJS(name, value, arguments)
	}
}

func vector(value any, call func(any) (any, error)) (any, error) {
	if values, ok := value.([]any); ok {
		result := make([]any, len(values))
		for i := range values {
			mapped, err := call(values[i])
			if err != nil {
				return nil, err
			}
			result[i] = mapped
		}
		return result, nil
	}
	return call(value)
}
func compact(value any) any {
	values, ok := value.([]any)
	if !ok {
		return value
	}
	result := make([]any, 0, len(values))
	for _, item := range values {
		if !isEmpty(item) {
			result = append(result, item)
		}
	}
	return result
}
func unique(value any) any {
	values, ok := value.([]any)
	if !ok {
		return value
	}
	seen := map[string]struct{}{}
	result := make([]any, 0, len(values))
	for _, item := range values {
		key, err := model.CanonicalJSON(item)
		if err != nil {
			continue
		}
		if _, found := seen[string(key)]; !found {
			seen[string(key)] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}
func flatten(value any) any {
	values, ok := value.([]any)
	if !ok {
		return value
	}
	var result []any
	for _, item := range values {
		if nested, ok := item.([]any); ok {
			result = append(result, nested...)
		} else {
			result = append(result, item)
		}
	}
	return result
}
func isEmpty(value any) bool {
	if _, ok := value.(missing); ok {
		return true
	}
	if value == nil {
		return true
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text) == ""
	}
	if values, ok := value.([]any); ok {
		return len(values) == 0
	}
	return false
}

func splitPipeline(expression string) ([]string, error) { return splitDelimited(expression, '|') }
func splitDelimited(input string, delimiter rune) ([]string, error) {
	var result []string
	start := 0
	depth := 0
	quote := rune(0)
	for i, char := range input {
		if quote != 0 {
			if char == quote {
				quote = 0
			}
			continue
		}
		switch char {
		case '\'', '"':
			quote = char
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, errors.New("unbalanced parentheses")
			}
		default:
			if char == delimiter && depth == 0 {
				result = append(result, strings.TrimSpace(input[start:i]))
				start = i + 1
			}
		}
	}
	if quote != 0 || depth != 0 {
		return nil, errors.New("unterminated quote or parentheses")
	}
	result = append(result, strings.TrimSpace(input[start:]))
	return result, nil
}
func parseCall(value string) (string, []string, error) {
	value = strings.TrimSpace(value)
	if !strings.Contains(value, "(") {
		return value, nil, nil
	}
	index := strings.Index(value, "(")
	if !strings.HasSuffix(value, ")") {
		return "", nil, fmt.Errorf("invalid function call %q", value)
	}
	arguments, err := splitDelimited(value[index+1:len(value)-1], ',')
	if err != nil {
		return "", nil, err
	}
	if len(arguments) == 1 && arguments[0] == "" {
		arguments = nil
	}
	return strings.TrimSpace(value[:index]), arguments, nil
}

func toMap(resource model.Resource) (map[string]any, error) {
	raw, err := json.Marshal(resource)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(raw, &result)
	return result, err
}
func setPointer(document map[string]any, pointer string, value any) error {
	parts, err := pointerParts(pointer)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return errors.New("mapping may not replace the root document")
	}
	current := document
	for _, part := range parts[:len(parts)-1] {
		next, ok := current[part].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[part] = next
		}
		current = next
	}
	current[parts[len(parts)-1]] = value
	return nil
}
func deletePointer(document map[string]any, pointer string) {
	parts, err := pointerParts(pointer)
	if err != nil || len(parts) == 0 {
		return
	}
	current := document
	for _, part := range parts[:len(parts)-1] {
		next, ok := current[part].(map[string]any)
		if !ok {
			return
		}
		current = next
	}
	delete(current, parts[len(parts)-1])
}
func pointerParts(pointer string) ([]string, error) {
	if !strings.HasPrefix(pointer, "/") {
		return nil, errors.New("JSON Pointer must begin with /")
	}
	raw := strings.Split(pointer[1:], "/")
	for i := range raw {
		raw[i] = strings.ReplaceAll(strings.ReplaceAll(raw[i], "~1", "/"), "~0", "~")
	}
	return raw, nil
}

type jsRuntime struct {
	runtime   *goja.Runtime
	functions map[string]goja.Callable
}

func (e *Engine) newRuntime() (*jsRuntime, error) {
	holder := &jsRuntime{runtime: goja.New(), functions: map[string]goja.Callable{}}
	if err := holder.runtime.Set("register", func(call goja.FunctionCall) goja.Value {
		name := call.Argument(0).String()
		if !strings.Contains(name, ".") {
			panic(holder.runtime.NewTypeError("custom function names must be namespaced"))
		}
		callable, ok := goja.AssertFunction(call.Argument(2))
		if !ok {
			panic(holder.runtime.NewTypeError("register requires a function as its third argument"))
		}
		holder.functions[name] = callable
		return goja.Undefined()
	}); err != nil {
		return nil, err
	}
	for _, script := range e.scripts {
		if _, err := holder.runtime.RunProgram(script.program); err != nil {
			return nil, fmt.Errorf("load %s: %w", script.name, err)
		}
	}
	return holder, nil
}
func (e *Engine) callJS(name string, value any, args []any) (result any, err error) {
	raw := e.pool.Get()
	defer func() {
		if holder, ok := raw.(*jsRuntime); ok {
			holder.runtime.ClearInterrupt()
			e.pool.Put(holder)
		}
	}()
	if initErr, ok := raw.(error); ok {
		return nil, initErr
	}
	holder := raw.(*jsRuntime)
	callable, ok := holder.functions[name]
	if !ok {
		return nil, fmt.Errorf("unknown JavaScript mapping function %q", name)
	}
	timer := time.AfterFunc(100*time.Millisecond, func() { holder.runtime.Interrupt("function deadline exceeded") })
	defer timer.Stop()
	values := make([]goja.Value, 0, len(args)+1)
	values = append(values, holder.runtime.ToValue(value))
	for _, arg := range args {
		values = append(values, holder.runtime.ToValue(arg))
	}
	output, callErr := callable(goja.Undefined(), values...)
	if callErr != nil {
		return nil, fmt.Errorf("JavaScript function %s: %w", name, callErr)
	}
	exported := output.Export()
	encoded, marshalErr := json.Marshal(exported)
	if marshalErr != nil {
		return nil, fmt.Errorf("JavaScript output must be JSON-safe: %w", marshalErr)
	}
	if len(encoded) > 1<<20 {
		return nil, errors.New("JavaScript output exceeds 1 MiB")
	}
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) serializableFields() map[string]any {
	result := map[string]any{}
	for kind, fields := range e.fields {
		current := map[string]any{}
		for pointer, field := range fields {
			current[pointer] = map[string]any{"expr": field.Expr, "literal": field.Literal, "remove": field.Remove}
		}
		result[kind] = current
	}
	return result
}

func (e *Engine) functionSources() []string {
	result := make([]string, len(e.scripts))
	for i, script := range e.scripts {
		result[i] = script.source
	}
	return result
}
