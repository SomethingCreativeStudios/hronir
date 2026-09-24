declare function register(
  name: `${string}.${string}`,
  metadata: { mode: "scalar" | "collection"; input: string; output: string },
  fn: (value: unknown, ...args: unknown[]) => unknown,
): void;
