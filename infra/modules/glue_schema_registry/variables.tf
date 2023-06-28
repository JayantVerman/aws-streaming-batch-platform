# AWS Glue Schema Registry module.

# Registers the Avro schemas the Kinesis producers serialize with. Schemas are
# plain text published into the registry (no extra Glue compute cost — this is
# effectively free at demo volumes).

variable "registry_name" {
  description = "Glue Schema Registry name"
  type        = string
  default     = "retail-schemas"
}

variable "schemas" {
  description = "map schema-name -> Avro schema definition (JSON). Read from streaming/schemas/*.avsc in the root."
  type        = map(string)
}

variable "compatibility" {
  description = "Schema compatibility mode (BACKWARD recommended for evolving streams)"
  type        = string
  default     = "BACKWARD"
}