// Hronir functions are local, trusted mapping helpers. They have no network,
// filesystem, environment, module, or timer APIs.
register("org.cleanCode", { mode: "scalar", input: "string", output: "string" }, function (value) {
  return String(value || "").trim().toUpperCase();
});
