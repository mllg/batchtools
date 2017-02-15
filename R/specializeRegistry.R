specializeRegistry = function(reg) {
  UseMethod("specializeRegistry")
}

specializeRegistry.default = function(reg) {
  reg
}
