using MySql.Data.MySqlClient;

namespace Resilead.Integration.Local.Infrastructure;

internal sealed class Db
{
    private readonly string _cs;
    public Db(string cs) => _cs = cs;
    public async Task<MySqlConnection> OpenAsync()
    {
        var c = new MySqlConnection(_cs);
        await c.OpenAsync();
        return c;
    }
}

