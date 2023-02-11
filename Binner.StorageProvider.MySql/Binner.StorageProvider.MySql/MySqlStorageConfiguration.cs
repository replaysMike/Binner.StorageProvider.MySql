namespace Binner.StorageProvider.MySql
{
    public class MySqlStorageConfiguration
    {
        public string? ConnectionString { get; set; }

        public MySqlStorageConfiguration()
        {
        }

        public MySqlStorageConfiguration(IDictionary<string, string> config)
        {
            if (config.ContainsKey("ConnectionString"))
                ConnectionString = config["ConnectionString"];
        }
    }
}
