using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using TypeSupport;
using TypeSupport.Extensions;

namespace Binner.StorageProvider.MySql
{
    public class MySqlSchemaGenerator<T>
    {
        private string _dbName;
        private ICollection<ExtendedProperty> _tables;

        public MySqlSchemaGenerator(string databaseName)
        {
            _dbName = databaseName;
            var properties = typeof(T).GetProperties(PropertyOptions.HasGetter);
            _tables = properties.Where(x => x.Type.IsCollection).ToList();
        }

        public string SetCharacterSet() => "SET character_set_results=utf8;\r\n";

        // note: the char set and collate settings are required to work with MariaDb ( CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci)
        public string CreateDatabaseIfNotExists() => $"CREATE DATABASE IF NOT EXISTS {_dbName};\r\n";

        public string CreateTableSchemaIfNotExists()
        {
            return $@"CREATE SCHEMA IF NOT EXISTS dbo;
{string.Join("\r\n", GetTableSchemas())}";
        }

        private ICollection<string> GetTableSchemas()
        {
            var tableSchemas = new List<string>();
            foreach (var tableProperty in _tables)
            {
                var tableExtendedType = tableProperty.Type;
                var columnProps = tableExtendedType.ElementType.GetProperties(PropertyOptions.HasGetter);
                var tableSchema = new List<string>();
                var tablePostSchemaText = new List<string>();
                foreach (var columnProp in columnProps)
                {
                    tableSchema.Add(GetColumnSchema(columnProp, out var postSchemaText, out var preTableText));
                    tablePostSchemaText.AddRange(postSchemaText);
                    if (preTableText.Any())
                        tableSchemas.Add(string.Join("\r\n", preTableText));
                }
                tableSchemas.Add(CreateTableIfNotExists(tableProperty.Name, string.Join(",\r\n", tableSchema), tablePostSchemaText));
            }
            return tableSchemas;
        }

        private string GetColumnSchema(ExtendedProperty prop, out List<string> postSchemaText, out List<string> preTableText)
        {
            postSchemaText = new List<string>();
            preTableText = new List<string>();
            var columnSchema = "";
            var propExtendedType = prop.Type;
            var maxLength = GetMaxLength(prop);
            if (propExtendedType.IsCollection)
            {
                // store as string, data will be comma delimited
                if (maxLength == "max")
                    columnSchema = $"{prop.Name} text";
                else
                    columnSchema = $"{prop.Name} varchar({maxLength})";
            }
            else
            {
                switch (propExtendedType)
                {
                    case var p when p.NullableBaseType == typeof(byte):
                        columnSchema = $"{prop.Name} tinyint";
                        break;
                    case var p when p.NullableBaseType == typeof(short):
                        columnSchema = $"{prop.Name} smallint";
                        break;
                    case var p when p.NullableBaseType == typeof(int):
                        columnSchema = $"{prop.Name} integer";
                        break;
                    case var p when p.NullableBaseType == typeof(long):
                        columnSchema = $"{prop.Name} bigint";
                        break;
                    case var p when p.NullableBaseType == typeof(double):
                        columnSchema = $"{prop.Name} float";
                        break;
                    case var p when p.NullableBaseType == typeof(decimal):
                        columnSchema = $"{prop.Name} decimal(18, 3)";
                        break;
                    case var p when p.NullableBaseType == typeof(string):
                        if (maxLength == "max")
                            columnSchema = $"{prop.Name} text";
                        else
                            columnSchema = $"{prop.Name} varchar({maxLength})";
                        break;
                    case var p when p.NullableBaseType == typeof(DateTime):
                        columnSchema = $"{prop.Name} timestamp";
                        break;
                    case var p when p.NullableBaseType == typeof(TimeSpan):
                        columnSchema = $"{prop.Name} time";
                        break;
                    case var p when p.NullableBaseType == typeof(byte[]):
                        if (maxLength == "max")
                            columnSchema = $"{prop.Name} varbinary(65535)";
                        else
                            columnSchema = $"{prop.Name} varbinary({maxLength})";
                        break;
                    default:
                        throw new InvalidOperationException($"Unsupported data type: {prop.Type}");
                }
            }
            if (prop.CustomAttributes.ToList().Any(x => x.AttributeType == typeof(KeyAttribute)))
            {
                if (propExtendedType.NullableBaseType != typeof(string) && propExtendedType.NullableBaseType.IsValueType)
                    columnSchema = columnSchema + " AUTO_INCREMENT";
                columnSchema = columnSchema + " NOT NULL";
                postSchemaText.Add($",\r\nPRIMARY KEY({prop.Name})");
            }
            else if (propExtendedType.Type != typeof(string) && !propExtendedType.IsNullable && !propExtendedType.IsCollection)
                columnSchema = columnSchema + " NOT NULL";
            return columnSchema;
        }

        private string GetMaxLength(ExtendedProperty prop)
        {
            var maxLengthAttr = prop.CustomAttributes.ToList().FirstOrDefault(x => x.AttributeType == typeof(MaxLengthAttribute));
            var maxLength = "max";
            if (maxLengthAttr != null)
            {
                maxLength = maxLengthAttr.ConstructorArguments.First().Value.ToString();
            }
            return maxLength;
        }

        private string CreateTableIfNotExists(string tableName, string tableSchema, List<string> postSchemaText)
        {
            var createTable = $@"CREATE TABLE IF NOT EXISTS {tableName} (
    {tableSchema}
";
            if (postSchemaText.Any())
                createTable += $"{string.Join("\r\n", postSchemaText)}";
            createTable += "\r\n);\r\n";
            return createTable;
        }
    }
}
