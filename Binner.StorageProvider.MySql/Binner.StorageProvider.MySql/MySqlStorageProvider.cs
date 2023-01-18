using Binner.Model.Common;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using TypeSupport.Extensions;
using static Binner.Model.Common.SystemDefaults;

namespace Binner.StorageProvider.MySql
{
    /// <summary>
    /// A storage provider for MySql
    /// </summary>
    public class MySqlStorageProvider : IStorageProvider
    {
        public const string ProviderName = "MySql";

        private readonly MySqlStorageConfiguration _config;
        private bool _isDisposed;

        public MySqlStorageProvider(IDictionary<string, string> config)
        {
            _config = new MySqlStorageConfiguration(config);
            try
            {
                GenerateDatabaseIfNotExistsAsync<IBinnerDb>()
                    .GetAwaiter()
                    .GetResult();
            }
            catch (Exception ex)
            {
                throw new StorageProviderException(nameof(MySqlStorageProvider), $"Failed to generate database! {ex.GetType().Name} = {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Get an instance of the entire database
        /// </summary>
        /// <returns></returns>
        public async Task<IBinnerDb> GetDatabaseAsync(IUserContext userContext)
        {
            var parts = await GetPartsAsync();
            return new BinnerDbV1
            {
                OAuthCredentials = await GetOAuthCredentialAsync(userContext),
                Parts = parts,
                PartTypes = await GetPartTypesAsync(userContext),
                Projects = await GetProjectsAsync(userContext),
                Count = parts.Count,
                FirstPartId = parts.OrderBy(x => x.PartId).First().PartId,
                LastPartId = parts.OrderBy(x => x.PartId).Last().PartId,
            };
        }

        public async Task<long> GetUniquePartsCountAsync(IUserContext userContext)
        {
            var query = $"SELECT COUNT(*) FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await ExecuteScalarAsync<long>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<long> GetPartsCountAsync(IUserContext userContext)
        {
            var query = $"SELECT CAST(SUM(Quantity) AS int) FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await ExecuteScalarAsync<int>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<decimal> GetPartsValueAsync(IUserContext userContext)
        {
            var query = $"SELECT SUM(Cost * Quantity) FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await ExecuteScalarAsync<decimal>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<Part>> GetLowStockAsync(PaginatedRequest request, IUserContext userContext)
        {
            var offsetRecords = (request.Page - 1) * request.Results;
            var sortDirection = request.Direction == SortDirection.Ascending ? "ASC" : "DESC";
            var query =
$@"SELECT * FROM Parts 
WHERE Quantity <= LowStockThreshold AND (@UserId IS NULL OR UserId = @UserId)
ORDER BY 
CASE WHEN @OrderBy IS NULL THEN PartId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartNumber' THEN PartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DigikeyPartNumber' THEN DigikeyPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'MouserPartNumber' THEN MouserPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Cost' THEN Cost ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Quantity' THEN Quantity ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'LowStockThreshold' THEN LowStockThreshold ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartTypeId' THEN PartTypeId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ProjectId' THEN ProjectId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Location' THEN Location ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber' THEN BinNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber2' THEN BinNumber2 ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Manufacturer' THEN Manufacturer ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ManufacturerPartNumber' THEN ManufacturerPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DateCreatedUtc' THEN DateCreatedUtc ELSE NULL END {sortDirection} 
OFFSET {offsetRecords} ROWS FETCH NEXT {request.Results} ROWS ONLY;";
            var result = await SqlQueryAsync<Part>(query, new
            {
                Results = request.Results,
                Page = request.Page,
                OrderBy = request.OrderBy,
                Direction = request.Direction,
                UserId = userContext?.UserId
            });
            return result;
        }

        public async Task<Part> AddPartAsync(Part part, IUserContext userContext)
        {
            part.UserId = userContext?.UserId;
            var query =
$@"INSERT INTO Parts (Quantity, LowStockThreshold, PartNumber, PackageType, MountingTypeId, DigiKeyPartNumber, MouserPartNumber, Description, PartTypeId, ProjectId, Keywords, DatasheetUrl, Location, BinNumber, BinNumber2, UserId, Cost, Manufacturer, ManufacturerPartNumber, LowestCostSupplier, LowestCostSupplierUrl, ProductUrl, ImageUrl, DateCreatedUtc) 
VALUES(@Quantity, @LowStockThreshold, @PartNumber, @PackageType, @MountingTypeId, @DigiKeyPartNumber, @MouserPartNumber, @Description, @PartTypeId, @ProjectId, @Keywords, @DatasheetUrl, @Location, @BinNumber, @BinNumber2, @UserId, @Cost, @Manufacturer, @ManufacturerPartNumber, @LowestCostSupplier, @LowestCostSupplierUrl, @ProductUrl, @ImageUrl, @DateCreatedUtc);";
            return await InsertAsync<Part, long>(query, part, (x, key) => { x.PartId = key; });
        }

        public async Task<Project> AddProjectAsync(Project project, IUserContext userContext)
        {
            project.UserId = userContext?.UserId;
            var query =
$@"INSERT INTO Projects (Name, Description, Location, Color, UserId, DateCreatedUtc) 
VALUES(@Name, @Description, @Location, @Color, @UserId, @DateCreatedUtc);";
            return await InsertAsync<Project, long>(query, project, (x, key) => { x.ProjectId = key; });
        }

        public async Task<bool> DeletePartAsync(Part part, IUserContext userContext)
        {
            part.UserId = userContext?.UserId;
            var query = $"DELETE FROM Parts WHERE PartId = @PartId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync<Part>(query, part) > 0;
        }

        public async Task<bool> DeletePartTypeAsync(PartType partType, IUserContext userContext)
        {
            partType.UserId = userContext?.UserId;
            var query = $"DELETE FROM PartTypes WHERE PartTypeId = @PartTypeId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync<PartType>(query, partType) > 0;
        }

        public async Task<bool> DeleteProjectAsync(Project project, IUserContext userContext)
        {
            project.UserId = userContext?.UserId;
            var query = $"DELETE FROM Projects WHERE ProjectId = @ProjectId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync<Project>(query, project) > 0;
        }

        public async Task<ICollection<SearchResult<Part>>> FindPartsAsync(string keywords, IUserContext userContext)
        {
            // basic ranked search by Michael Brown :)
            var query =
$@"WITH PartsExactMatch (PartId, Rank) AS
(
SELECT PartId, 10 as Rank FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId) AND 
PartNumber = @Keywords 
OR DigiKeyPartNumber = @Keywords 
OR MouserPartNumber = @Keywords
OR ManufacturerPartNumber = @Keywords
OR Description = @Keywords 
OR Keywords = @Keywords 
OR Location = @Keywords 
OR BinNumber = @Keywords 
OR BinNumber2 = @Keywords
),
PartsBeginsWith (PartId, Rank) AS
(
SELECT PartId, 100 as Rank FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId) AND 
PartNumber LIKE CONCAT(@Keywords, '%')
OR DigiKeyPartNumber LIKE CONCAT(@Keywords, '%')
OR MouserPartNumber LIKE CONCAT(@Keywords, '%')
OR ManufacturerPartNumber LIKE CONCAT(@Keywords, '%')
OR Description LIKE CONCAT(@Keywords, '%')
OR Keywords LIKE CONCAT(@Keywords, '%')
OR Location LIKE CONCAT(@Keywords, '%')
OR BinNumber LIKE CONCAT(@Keywords, '%')
OR BinNumber2 LIKE CONCAT(@Keywords, '%')
),
PartsAny (PartId, Rank) AS
(
SELECT PartId, 200 as Rank FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId) AND 
PartNumber LIKE CONCAT('%', @Keywords, '%')
OR DigiKeyPartNumber LIKE CONCAT('%', @Keywords, '%')
OR MouserPartNumber LIKE CONCAT('%', @Keywords, '%')
OR ManufacturerPartNumber LIKE CONCAT('%', @Keywords, '%')
OR Description LIKE CONCAT('%', @Keywords, '%')
OR Keywords LIKE CONCAT('%', @Keywords, '%')
OR Location LIKE CONCAT('%', @Keywords, '%')
OR BinNumber LIKE CONCAT('%', @Keywords, '%')
OR BinNumber2 LIKE CONCAT('%', @Keywords, '%')
),
PartsMerged (PartId, Rank) AS
(
	SELECT PartId, Rank FROM PartsExactMatch
	UNION
	SELECT PartId, Rank FROM PartsBeginsWith
	UNION
	SELECT PartId, Rank FROM PartsAny
)
SELECT pm.Rank, p.* FROM Parts p
INNER JOIN (
  SELECT PartId, MIN(Rank) Rank FROM PartsMerged GROUP BY PartId
) pm ON pm.PartId = p.PartId ORDER BY pm.Rank ASC;
;";
            var result = await SqlQueryAsync<PartSearch>(query, new { Keywords = keywords, UserId = userContext?.UserId });
            return result.Select(x => new SearchResult<Part>(x as Part, x.Rank)).OrderBy(x => x.Rank).ToList();
        }

        private async Task<ICollection<OAuthCredential>> GetOAuthCredentialAsync(IUserContext userContext)
        {
            var query = $"SELECT * FROM OAuthCredentials WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<OAuthCredential>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<OAuthCredential> GetOAuthCredentialAsync(string providerName, IUserContext userContext)
        {
            var query = $"SELECT * FROM OAuthCredentials WHERE Provider = @ProviderName AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<OAuthCredential>(query, new { ProviderName = providerName, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<PartType> GetOrCreatePartTypeAsync(PartType partType, IUserContext userContext)
        {
            partType.UserId = userContext?.UserId;
            var query = $"SELECT PartTypeId FROM PartTypes WHERE Name = @Name AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PartType>(query, partType);
            if (result.Any())
            {
                return result.FirstOrDefault();
            }
            else
            {
                query =
$@"INSERT INTO PartTypes (ParentPartTypeId, Name, UserId, DateCreatedUtc) 
output INSERTED.PartTypeId 
VALUES (@ParentPartTypeId, @Name, @UserId, @DateCreatedUtc);";
                partType = await InsertAsync<PartType, long>(query, partType, (x, key) => { x.PartTypeId = key; });
            }
            return partType;
        }

        public async Task<ICollection<PartType>> GetPartTypesAsync(IUserContext userContext)
        {
            var query = $"SELECT * FROM PartTypes WHERE (@UserId IS NULL OR UserId = @UserId) OR UserId IS NULL;";
            var result = await SqlQueryAsync<PartType>(query, new { UserId = userContext?.UserId });
            return result.ToList();
        }

        public async Task<Part> GetPartAsync(long partId, IUserContext userContext)
        {
            var query = $"SELECT * FROM Parts WHERE PartId = @PartId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Part>(query, new { PartId = partId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<Part> GetPartAsync(string partNumber, IUserContext userContext)
        {
            var query = $"SELECT * FROM Parts WHERE PartNumber = @PartNumber AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Part>(query, new { PartNumber = partNumber, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        private async Task<ICollection<Part>> GetPartsAsync()
        {
            var query = $"SELECT * FROM Parts;";
            var result = await SqlQueryAsync<Part>(query);
            return result;
        }

        public async Task<ICollection<Part>> GetPartsAsync(Expression<Func<Part, bool>> predicate, IUserContext userContext)
        {
            var conditionalQuery = TranslatePredicateToSql(predicate);
            var sql = conditionalQuery.Sql.Replace("[", "").Replace("]", "");
            var query = $"SELECT * FROM Parts WHERE {sql} AND (@UserId IS NULL OR UserId = @UserId);";
            conditionalQuery.Parameters.Add("UserId", userContext?.UserId);
            var result = await SqlQueryAsync<Part>(query, conditionalQuery.Parameters);
            return result.ToList();
        }

        private WhereCondition TranslatePredicateToSql(Expression<Func<Part, bool>> predicate)
        {
            var builder = new SqlWhereExpressionBuilder();
            var sql = builder.ToParameterizedSql<Part>(predicate);
            return sql;
        }

        public async Task<ICollection<Part>> GetPartsAsync(PaginatedRequest request, IUserContext userContext)
        {
            var offsetRecords = (request.Page - 1) * request.Results;
            var sortDirection = request.Direction == SortDirection.Ascending ? "ASC" : "DESC";
            var binFilter = "";

            if (request.By != null)
            {
                binFilter = $" AND {request.By} = '{request.Value}'";
            }

            var query =
$@"SELECT * FROM Parts 
WHERE (@UserId IS NULL OR UserId = @UserId) {binFilter}
ORDER BY 
CASE WHEN @OrderBy IS NULL THEN PartId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartNumber' THEN PartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DigikeyPartNumber' THEN DigikeyPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'MouserPartNumber' THEN MouserPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Cost' THEN Cost ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Quantity' THEN Quantity ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'LowStockThreshold' THEN LowStockThreshold ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartTypeId' THEN PartTypeId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ProjectId' THEN ProjectId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Location' THEN Location ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber' THEN BinNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber2' THEN BinNumber2 ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Manufacturer' THEN Manufacturer ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ManufacturerPartNumber' THEN ManufacturerPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DateCreatedUtc' THEN DateCreatedUtc ELSE NULL END {sortDirection} 
OFFSET {offsetRecords} ROWS FETCH NEXT {request.Results} ROWS ONLY;";
            var result = await SqlQueryAsync<Part>(query, new
            {
                Results = request.Results,
                Page = request.Page,
                OrderBy = request.OrderBy,
                Direction = request.Direction,
                UserId = userContext?.UserId
            });
            return result.ToList();
        }

        public async Task<PartType> GetPartTypeAsync(long partTypeId, IUserContext userContext)
        {
            var query = $"SELECT * FROM PartTypes WHERE PartTypeId = @PartTypeId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PartType>(query, new { PartTypeId = partTypeId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<Project> GetProjectAsync(long projectId, IUserContext userContext)
        {
            var query = $"SELECT * FROM Projects WHERE ProjectId = @ProjectId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Project>(query, new { ProjectId = projectId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<Project> GetProjectAsync(string projectName, IUserContext userContext)
        {
            var query = $"SELECT * FROM Projects WHERE Name = @Name AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Project>(query, new { Name = projectName, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        private async Task<ICollection<Project>> GetProjectsAsync(IUserContext userContext)
        {
            var query = $@"SELECT * FROM Projects WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Project>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<Project>> GetProjectsAsync(PaginatedRequest request, IUserContext userContext)
        {
            var offsetRecords = (request.Page - 1) * request.Results;
            var sortDirection = request.Direction == SortDirection.Ascending ? "ASC" : "DESC";
            var query =
$@"SELECT * FROM Projects 
WHERE (@UserId IS NULL OR UserId = @UserId) 
ORDER BY 
CASE WHEN @OrderBy IS NULL THEN ProjectId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Name' THEN Name ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Description' THEN Description ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Location' THEN Location ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DateCreatedUtc' THEN DateCreatedUtc ELSE NULL END {sortDirection} 
OFFSET {offsetRecords} ROWS FETCH NEXT {request.Results} ROWS ONLY;";
            var result = await SqlQueryAsync<Project>(query, new
            {
                Results = request.Results,
                Page = request.Page,
                OrderBy = request.OrderBy,
                Direction = request.Direction,
                UserId = userContext?.UserId
            });
            return result.ToList();
        }

        public async Task RemoveOAuthCredentialAsync(string providerName, IUserContext userContext)
        {
            var query = $"DELETE FROM OAuthCredentials WHERE Provider = @Provider AND (@UserId IS NULL OR UserId = @UserId);";
            await ExecuteAsync<object>(query, new { Provider = providerName, UserId = userContext?.UserId });
        }

        public async Task<OAuthCredential> SaveOAuthCredentialAsync(OAuthCredential credential, IUserContext userContext)
        {
            credential.UserId = userContext?.UserId;
            var query = @"SELECT Provider FROM OAuthCredentials WHERE Provider = @Provider AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<OAuthCredential>(query, credential);
            if (result.Any())
            {
                query = $@"UPDATE OAuthCredentials SET AccessToken = @AccessToken, RefreshToken = @RefreshToken, DateCreatedUtc = @DateCreatedUtc, DateExpiresUtc = @DateExpiresUtc WHERE Provider = @Provider AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync<object>(query, credential);
            }
            else
            {
                query =
$@"INSERT INTO OAuthCredentials (Provider, AccessToken, RefreshToken, DateCreatedUtc, DateExpiresUtc, UserId) 
VALUES (@Provider, @AccessToken, @RefreshToken, @DateCreatedUtc, @DateExpiresUtc, @UserId);";
                await InsertAsync<OAuthCredential, string>(query, credential, (x, key) => { });
            }
            return credential;
        }

        public async Task<Part> UpdatePartAsync(Part part, IUserContext userContext)
        {
            part.UserId = userContext?.UserId;
            var query = $"SELECT PartId FROM Parts WHERE PartId = @PartId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Part>(query, part);
            if (result.Any())
            {
                query = $"UPDATE Parts SET Quantity = @Quantity, LowStockThreshold = @LowStockThreshold, Cost = @Cost, PartNumber = @PartNumber, PackageType = @PackageType, MountingTypeId = @MountingTypeId, DigiKeyPartNumber = @DigiKeyPartNumber, MouserPartNumber = @MouserPartNumber, Description = @Description, PartTypeId = @PartTypeId, ProjectId = @ProjectId, Keywords = @Keywords, DatasheetUrl = @DatasheetUrl, Location = @Location, BinNumber = @BinNumber, BinNumber2 = @BinNumber2, ProductUrl = @ProductUrl, ImageUrl = @ImageUrl, LowestCostSupplier = @LowestCostSupplier, LowestCostSupplierUrl = @LowestCostSupplierUrl, Manufacturer = @Manufacturer, ManufacturerPartNumber = @ManufacturerPartNumber WHERE PartId = @PartId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync<Part>(query, part);
            }
            else
            {
                throw new StorageProviderException(nameof(MySqlStorageProvider), $"Record not found for {nameof(Part)} = {part.PartId}");
            }
            return part;
        }

        public async Task<PartType> UpdatePartTypeAsync(PartType partType, IUserContext userContext)
        {
            partType.UserId = userContext?.UserId;
            var query = $"SELECT PartTypeId FROM PartTypes WHERE PartTypeId = @PartTypeId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PartType>(query, partType);
            if (result.Any())
            {
                query = $"UPDATE PartTypes SET Name = @Name, ParentPartTypeId = @ParentPartTypeId WHERE PartTypeId = @PartTypeId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync<PartType>(query, partType);
            }
            else
            {
                throw new StorageProviderException(nameof(MySqlStorageProvider), $"Record not found for {nameof(PartType)} = {partType.PartTypeId}");
            }
            return partType;
        }

        public async Task<Project> UpdateProjectAsync(Project project, IUserContext userContext)
        {
            project.UserId = userContext?.UserId;
            var query = $"SELECT ProjectId FROM Projects WHERE ProjectId = @ProjectId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Project>(query, project);
            if (result.Any())
            {
                query = $"UPDATE Projects SET Name = @Name, Description = @Description, Location = @Location, Color = @Color WHERE ProjectId = @ProjectId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync<Project>(query, project);
            }
            else
            {
                throw new StorageProviderException(nameof(MySqlStorageProvider), $"Record not found for {nameof(Project)} = {project.ProjectId}");
            }
            return project;
        }

        private async Task<T> InsertAsync<T, TKey>(string query, T parameters, Action<T, TKey> keySetter)
        {
            using (var connection = new MySqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new MySqlCommand(query, connection))
                {
                    sqlCmd.Parameters.AddRange(CreateParameters<T>(parameters));
                    sqlCmd.CommandType = CommandType.Text;
                    await sqlCmd.ExecuteNonQueryAsync();
                    var result = sqlCmd.LastInsertedId;
                    keySetter.Invoke(parameters, (TKey)Convert.ChangeType(result, typeof(TKey)));

                }
                connection.Close();
            }
            return parameters;
        }

        private async Task<ICollection<T>> SqlQueryAsync<T>(string query, object parameters = null)
        {
            var results = new List<T>();
            var type = typeof(T).GetExtendedType();
            using (var connection = new MySqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new MySqlCommand(query, connection))
                {
                    if (parameters != null)
                        sqlCmd.Parameters.AddRange(CreateParameters(parameters));
                    sqlCmd.CommandType = CommandType.Text;
                    var reader = await sqlCmd.ExecuteReaderAsync();
                    while (reader.Read())
                    {
                        var newObj = Activator.CreateInstance<T>();
                        foreach (var prop in type.Properties)
                        {
                            if (reader.HasColumn(prop.Name))
                            {
                                var val = MapToPropertyValue(reader[prop.Name], prop.Type);
                                newObj.SetPropertyValue(prop.PropertyInfo, val);
                            }
                        }
                        results.Add(newObj);
                    }
                }
                connection.Close();
            }
            return results;
        }

        private async Task<T> ExecuteScalarAsync<T>(string query, object parameters = null)
        {
            T result;
            using (var connection = new MySqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new MySqlCommand(query, connection))
                {
                    sqlCmd.Parameters.AddRange(CreateParameters(parameters));
                    sqlCmd.CommandType = CommandType.Text;
                    var untypedResult = await sqlCmd.ExecuteScalarAsync();
                    if (untypedResult != DBNull.Value)
                        result = (T)untypedResult;
                    else
                        result = default(T);
                }
                connection.Close();
            }
            return result;
        }

        private async Task<int> ExecuteAsync<T>(string query, T record)
        {
            var modified = 0;
            using (var connection = new MySqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new MySqlCommand(query, connection))
                {
                    sqlCmd.Parameters.AddRange(CreateParameters<T>(record));
                    sqlCmd.CommandType = CommandType.Text;
                    modified = await sqlCmd.ExecuteNonQueryAsync();
                }
                connection.Close();
            }
            return modified;
        }

        private MySqlParameter[] CreateParameters<T>(T record)
        {
            var parameters = new List<MySqlParameter>();
            var extendedType = record.GetExtendedType();
            if (extendedType.IsDictionary)
            {
                var t = record as IDictionary<string, object>;
                foreach (var p in t)
                {
                    var key = p.Key;
                    var val = p.Value;
                    var propertyMapped = MapFromPropertyValue(val);
                    parameters.Add(new MySqlParameter(key.ToString(), propertyMapped));
                }
            }
            else
            {
                var properties = record.GetProperties(PropertyOptions.HasGetter);
                foreach (var property in properties)
                {
                    var propertyValue = record.GetPropertyValue(property);
                    var propertyMapped = MapFromPropertyValue(propertyValue);
                    parameters.Add(new MySqlParameter(property.Name, propertyMapped));
                }
            }
            return parameters.ToArray();
        }

        private object MapToPropertyValue(object obj, Type destinationType)
        {
            if (obj == DBNull.Value) return null;

            var objType = destinationType.GetExtendedType();
            switch (objType)
            {
                case var p when p.IsCollection:
                case var a when a.IsArray:
                    return obj.ToString().Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
                default:
                    return obj;
            }
        }

        private object MapFromPropertyValue(object obj)
        {
            if (obj == null) return DBNull.Value;

            var objType = obj.GetExtendedType();
            switch (objType)
            {
                case var p when p.IsCollection:
                case var a when a.IsArray:
                    return string.Join(",", ((ICollection<string>)obj).Select(x => x.ToString()).ToArray());
                case var p when p.Type == typeof(DateTime):
                    if (((DateTime)obj) == DateTime.MinValue)
                        return SqlDateTime.MinValue.Value;
                    return obj;
                default:
                    return obj;
            }
        }

        private async Task<bool> GenerateDatabaseIfNotExistsAsync<T>()
        {
            var connectionStringBuilder = new MySqlConnectionStringBuilder(_config.ConnectionString);
            var schemaGenerator = new MySqlSchemaGenerator<T>(connectionStringBuilder.Database);
            var modified = 0;
            var query = string.Empty;

            // Ensure database exists
            using (var connection = new MySqlConnection(GetMasterDbConnectionString(_config.ConnectionString)))
            {
                connection.Open();
                query = schemaGenerator.CreateDatabaseIfNotExists();
                using (var sqlCmd = new MySqlCommand(query, connection))
                {
                    modified += await sqlCmd.ExecuteNonQueryAsync();
                }
                connection.Close();
            }
            // Ensure table schema exists
            query = schemaGenerator.CreateTableSchemaIfNotExists();
            using (var connection = new MySqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new MySqlCommand(query, connection))
                {
                    modified += await sqlCmd.ExecuteNonQueryAsync();
                }
                connection.Close();
            }
            if (modified > 0) await SeedInitialDataAsync();

            return modified > 0;
        }

        private string SetCharacterSet() => "SET character_set_results=utf8;\r\n";

        private async Task<bool> SeedInitialDataAsync()
        {
            //DefaultPartTypes
            var defaultPartTypes = typeof(SystemDefaults.DefaultPartTypes).GetExtendedType();
            var query = "";
            var modified = 0;
            foreach (var partType in defaultPartTypes.EnumValues)
            {
                int? parentPartTypeId = null;
                var partTypeEnum = (DefaultPartTypes)partType.Key;
                var field = typeof(DefaultPartTypes).GetField(partType.Value);
                if (field.IsDefined(typeof(ParentPartTypeAttribute), false))
                {
                    var customAttribute = Attribute.GetCustomAttribute(field, typeof(ParentPartTypeAttribute)) as ParentPartTypeAttribute;
                    parentPartTypeId = (int)customAttribute.Parent;
                }

                query += $"INSERT INTO PartTypes (Name, ParentPartTypeId, DateCreatedUtc) VALUES('{partType.Value}', {parentPartTypeId?.ToString() ?? "null"}, UTC_TIMESTAMP());\r\n";
            }
            using (var connection = new MySqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new MySqlCommand(query, connection))
                {
                    modified = await sqlCmd.ExecuteNonQueryAsync();
                }
                connection.Close();
            }
            return modified > 0;
        }

        private string GetMasterDbConnectionString(string connectionString)
        {
            var builder = new MySqlConnectionStringBuilder(connectionString);
            builder.Database = "mysql";
            return builder.ToString();
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool isDisposing)
        {
            if (_isDisposed)
                return;
            if (isDisposing)
            {

            }
            _isDisposed = true;
        }
    }
}
