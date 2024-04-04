using MapReduce;

int split = 10;
int N = 10000;
char[] alphabet = "abcdefghijklmnopqrstuvwxyz".ToCharArray();
int minSize = 3;
int maxSize = 7;
string outputPath = "input_data";

var fileGenerator = new FileGenerator(split, N, alphabet, minSize, maxSize);
//fileGenerator.GenerateFile(outputPath);

//Console.WriteLine($"Arquivo gerado em {outputPath}");

string command = Console.ReadLine();
string[] commandStrings = command.Split(' ', StringSplitOptions.RemoveEmptyEntries);
if (commandStrings[0] == "grep")
{
    bool isRegex = false;
    if (commandStrings[1] == "-e" && commandStrings.Length == 4) isRegex = true;

    string pattern = commandStrings[isRegex ? 2 : 1];
    if(isRegex) pattern = pattern.Substring(1, pattern.Length - 2);
    string path = commandStrings[isRegex ? 3 : 2];
    fileGenerator.SplitFile(outputPath, path);

    var inputData = new List<string>();
    for (int i = 1; i <= split; i++)
    {
        string filePath = $"input_data/file_{i}.txt";
        inputData.Add(File.ReadAllText(filePath));
    }

    int numWorkers = 5;

    var wordCountMapReduce = new GrepMapReduce(pattern, isRegex, numWorkers, "input_data/grep_temp.txt", "input_data/grep_output.txt");
    wordCountMapReduce.Run(inputData);
}
else
{
    Console.WriteLine("Comando inválido");
}
