import type { RelationMetadata } from "./domain";

export function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

export function relationReference(relation: RelationMetadata): string {
  return [
    quoteIdentifier(relation.database),
    quoteIdentifier(relation.schema),
    quoteIdentifier(relation.name),
  ].join(".");
}

export function previewQuery(relation: RelationMetadata, limit = 100): string {
  return `SELECT *\nFROM ${relationReference(relation)}\nLIMIT ${limit};`;
}

export function describeQuery(relation: RelationMetadata): string {
  return `DESCRIBE ${relationReference(relation)};`;
}

export function countQuery(relation: RelationMetadata): string {
  return `SELECT COUNT(*) AS row_count\nFROM ${relationReference(relation)};`;
}

export function showSchemaTablesQuery(database: string, schema: string): string {
  return `SHOW TABLES FROM ${quoteIdentifier(database)}.${quoteIdentifier(schema)};`;
}
