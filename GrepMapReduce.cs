using System.Text.RegularExpressions;

namespace MapReduce
{
    public class GrepMapReduce : MapReduce
    {
        private string Pattern;
        private bool IsRegex;

        public GrepMapReduce(string pattern, bool isRegex, int numWorkers, string tempFilePath, string outputFilePath) : base(numWorkers, tempFilePath, outputFilePath)
        {
            Pattern = pattern;
            IsRegex = isRegex;
        }

        protected override void Map(string input, int order)
        {
            foreach (var line in input.Split("\n", StringSplitOptions.RemoveEmptyEntries))
            {
                bool isValid = false;
                string trimmedLine = line.Trim();

                Regex regex = new Regex(Pattern);

                if (IsRegex && regex.IsMatch(trimmedLine))
                    isValid = true;

                if (!IsRegex && trimmedLine.Contains(Pattern))
                    isValid = true;

                if(isValid)
                {
                    lock (fileLock)
                    {
                        using (StreamWriter writer = new StreamWriter(TempFilePath, true))
                        {
                            writer.WriteLine($"{order} {trimmedLine}");
                        }
                    }
                }
            }
        }

        protected override void Reduce(List<string> strings)
        {
            foreach (var item in strings)
            {
                lock (fileLock)
                {
                    using (StreamWriter writer = new StreamWriter(OutputFilePath, true))
                    {
                        writer.WriteLine(item);
                    }
                }
            } 
        }
    }
}
