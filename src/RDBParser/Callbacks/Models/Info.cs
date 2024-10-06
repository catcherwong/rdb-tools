namespace RDBParser
{
    public class Info
    { 
        public string Encoding { get; set; }

        public ulong Idle { get; set; }

        public int Freq { get; set; }

        public int SizeOfValue { get; set; }

        public ulong Zips { get; set; }

        public ulong SlotId { get; set; }

        public override string ToString()
        {
            return $"Info{{Encoding={Encoding},Idle={Idle},Freq={Freq},SizeOfValue={SizeOfValue},Zips={Zips}}}";
        }
    }
}
