namespace MapReduce
{
    public class FileGenerator
    {
        private readonly int Split;
        private readonly int N;
        private readonly char[] Alphabet;
        private readonly int MinSize;
        private readonly int MaxSize;

        public FileGenerator(int split, int n, char[] alphabet, int minSize, int maxSize)
        {
            Split = split;
            N = n;
            Alphabet = alphabet;
            MinSize = minSize;
            MaxSize = maxSize;
        }

        public void GenerateFiles(string outputPath)
        {
            if (!Directory.Exists(outputPath))
            {
                Directory.CreateDirectory(outputPath);
            }

            string[] words = GenerateWords();
            string[] filesContent = SplitWords(words);

            for (int i = 0; i < Split; i++)
            {
                string filePath = Path.Combine(outputPath, $"file_{i + 1}.txt");
                File.WriteAllText(filePath, filesContent[i]);
            }
        }

        public void GenerateFile(string outputPath)
        {
            if (!Directory.Exists(outputPath))
            {
                Directory.CreateDirectory(outputPath);
            }

            string[] words = GenerateWords();

            string filePath = Path.Combine(outputPath, $"data.txt");
            File.WriteAllText(filePath, string.Join(" ", words));
        }

        private string[] GenerateWords()
        {
            var random = new Random();
            string[] words = new string[N];

            for (int i = 0; i < N; i++)
            {
                int wordSize = random.Next(MinSize, MaxSize + 1);
                char[] wordChars = new char[wordSize];

                for (int j = 0; j < wordSize; j++)
                {
                    wordChars[j] = Alphabet[random.Next(Alphabet.Length)];
                }

                words[i] = new string(wordChars);
            }

            return words;
        }

        private string[] SplitWords(string[] words)
        {
            int wordsPerSplit = (int)Math.Ceiling((double)N / Split);
            string[] splits = new string[Split];

            for (int i = 0; i < Split; i++)
            {
                string[] aux = words.Skip(i * wordsPerSplit)
                                    .Take(wordsPerSplit)
                                    .ToArray();

                splits[i] = string.Join(" ", aux);
            }

            return splits;
        }

        public void SplitFile(string outputPath, string path)
        {
            string file = File.ReadAllText($"{outputPath}/{path}");
            string[] lines = file.Split("\n", StringSplitOptions.RemoveEmptyEntries);
            int linesPerSplit = (int)Math.Ceiling((double)lines.Length / Split);

            for (int i = 0; i < Split; i++)
            {
                string[] aux = lines.Skip(i * linesPerSplit)
                                    .Take(linesPerSplit)
                                    .ToArray();

                string filePath = Path.Combine(outputPath, $"file_{i + 1}.txt");
                File.Delete(filePath);
                using (StreamWriter writer = new StreamWriter(filePath, true))
                {
                    writer.WriteLine(string.Join("\n", aux));
                }
            }
        }
    }
}
