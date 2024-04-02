namespace MapReduce
{
    public abstract class MapReduce
    {
        private readonly int NumWorkers;
        protected readonly string TempFilePath;
        protected readonly string OutputFilePath;
        protected readonly Dictionary<string, int> ReducerKeyValuePairs;
        private readonly Queue<string> MapQueue;
        private int Order = 1;

        protected static readonly object fileLock = new object();

        public MapReduce(int numWorkers, string tempFilePath, string outputFilePath)
        {
            NumWorkers = numWorkers;
            MapQueue = new Queue<string>();
            ReducerKeyValuePairs = new Dictionary<string, int>();
            TempFilePath = tempFilePath;
            OutputFilePath = outputFilePath;
            File.Delete(TempFilePath);
            File.Delete(OutputFilePath);
        }

        public void Run(IEnumerable<string> input)
        {
            foreach (var item in input)
            {
                MapQueue.Enqueue(item);
            }

            var mapTasks = new List<Task>();
            for (int i = 0; i < NumWorkers; i++)
            {
                mapTasks.Add(Task.Run(MapWorker));
            }

            Task.WaitAll(mapTasks.ToArray());

            var tempFileData = File.ReadAllLines(TempFilePath);

            var groupedData = GroupByKey(tempFileData);

            var orderedData = new List<string>[input.Count()];

            foreach (var item in groupedData)
            {
                orderedData[int.Parse(item.Key) - 1] = item.Value;
            }

            foreach (var item in orderedData)
            {
                if(item != null)
                    Reduce(item);
            }

            Console.WriteLine("Finalizou");
        }

        protected virtual void Map(string input, int order)
        {
            throw new NotImplementedException();
        }

        protected virtual void Reduce(List<string> strings)
        {
            throw new NotImplementedException();
        }

        private void MapWorker()
        {
            while (MapQueue.Count > 0)
            {
                try
                {
                    string item = MapQueue.Dequeue();
                    Map(item, Order++);
                }
                catch (InvalidOperationException)
                {
                }
            }
        }

        private static Dictionary<string, List<string>> GroupByKey(string[] tempFileData)
        {
            var groupedData = new Dictionary<string, List<string>>();

            foreach (var pair in tempFileData)
            {
                var pairSplited = pair.Split();
                if (!groupedData.TryGetValue(pairSplited[0], out List<string>? value))
                {
                    value = new List<string>();
                    groupedData[pairSplited[0]] = value;
                }

                value.Add(string.Join(" ", pairSplited.Skip(1).ToList()));
            }

            return groupedData;
        }
    }
}

