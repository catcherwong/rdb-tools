using System.Threading.Tasks;

namespace RDBParser
{
    public interface IRDBParser
    {
        void Parse(string path);

        Task ParseAsync(string path);
    }
}
