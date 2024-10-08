using System;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Runtime.InteropServices;

class Program
{

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern IntPtr CreateJobObject(IntPtr lpJobAttributes, string lpName);

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool SetInformationJobObject(IntPtr hJob, JobObjectInfoType infoType, IntPtr lpJobObjectInfo, uint cbJobObjectInfoLength);

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool AssignProcessToJobObject(IntPtr hJob, IntPtr hProcess);

    [StructLayout(LayoutKind.Sequential)]
    public struct JOBOBJECT_BASIC_LIMIT_INFORMATION
    {
        public long PerProcessUserTimeLimit;
        public long PerJobUserTimeLimit;
        public uint LimitFlags;
        public UIntPtr MinimumWorkingSetSize;
        public UIntPtr MaximumWorkingSetSize;
        public uint ActiveProcessLimit;
        public IntPtr Affinity;
        public uint PriorityClass;
        public uint SchedulingClass;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct IO_COUNTERS
    {
        public ulong ReadOperationCount;
        public ulong WriteOperationCount;
        public ulong OtherOperationCount;
        public ulong ReadTransferCount;
        public ulong WriteTransferCount;
        public ulong OtherTransferCount;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
    {
        public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
        public IO_COUNTERS IoInfo;
        public UIntPtr ProcessMemoryLimit;
        public UIntPtr JobMemoryLimit;
        public UIntPtr PeakProcessMemoryUsed;
        public UIntPtr PeakJobMemoryUsed;
    }

    public enum JobObjectInfoType
    {
        JobObjectBasicLimitInformation = 2,
        JobObjectExtendedLimitInformation = 9
    }

    public const uint JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x100;
    public const uint JOB_OBJECT_LIMIT_JOB_MEMORY = 0x200;

    public class ExternalSort
    {
        public static void MergeFiles(string outputFile, int k, int bufferSize = 1024 * 1024)
        {
            var minHeap = new SortedSet<(int Element, int Index)>();

            using (var outStream = new BufferedStream(new FileStream(outputFile, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize)))
            using (var writer = new StreamWriter(outStream))
            {
                var readers = new StreamReader[k];
                for (int i = 0; i < k; i++)
                {
                    var fileStream = new FileStream(i.ToString(), FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize);
                    var bufferedStream = new BufferedStream(fileStream, bufferSize);
                    readers[i] = new StreamReader(bufferedStream);

                    string line = readers[i].ReadLine();
                    if (line != null)
                    {
                        minHeap.Add((int.Parse(line), i));
                    }
                }

                while (minHeap.Count > 0)
                {
                    var root = minHeap.Min;
                    minHeap.Remove(root);

                    writer.WriteLine(root.Element);

                    string line = readers[root.Index].ReadLine();
                    if (line != null)
                    {
                        minHeap.Add((int.Parse(line), root.Index));
                    }
                }

                for (int i = 0; i < k; i++)
                {
                    readers[i].Close();
                }

                Parallel.For(0, k, (i) =>
                {
                    File.Delete(i.ToString());
                });
            }
        }

        public static void CreateInitialRuns(string inputFile, long runSizeBytes, int numWays, int bufferSize = 1024 * 1024)
        {
            using (var inStream = new BufferedStream(new FileStream(inputFile, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize)))
            using (var reader = new StreamReader(inStream))
            {
                var tempFile = new StreamWriter[numWays];
                for (int i = 0; i < numWays; i++)
                {
                    var fileStream = new FileStream(i.ToString(), FileMode.Create, FileAccess.Write, FileShare.None, bufferSize);
                    var bufferedStream = new BufferedStream(fileStream, bufferSize);
                    tempFile[i] = new StreamWriter(bufferedStream);
                }

                bool moreInputLeft = true;
                int nextOutputFile = 0;

                while (moreInputLeft)
                {
                    var tempReadNum = new List<int>();
                    long currentRunSize = 0;

                    while (currentRunSize < runSizeBytes)
                    {
                        string line = reader.ReadLine();
                        if (line != null)
                        {
                            tempReadNum.Add(int.Parse(line));
                            currentRunSize += System.Text.Encoding.UTF8.GetByteCount(line);
                        }
                        else
                        {
                            moreInputLeft = false;
                            break;
                        }
                    }

                    tempReadNum.Sort();
                    using (var writer = tempFile[nextOutputFile])
                    {
                        foreach (var item in tempReadNum)
                        {
                            writer.WriteLine(item);
                        }
                    }

                    nextOutputFile = (nextOutputFile + 1) % numWays;
                }

                for (int i = 0; i < numWays; i++)
                {
                    tempFile[i].Close();
                }
            }
        }

        public static void ExternalSortFile(string inputFile, string outputFile, int numWays, long runSize)
        {
            CreateInitialRuns(inputFile, runSize, numWays);
            MergeFiles(outputFile, numWays);
        }
    }

    static void Main(string[] args)
    {
        IntPtr hJob = CreateJobObject(IntPtr.Zero, null);
        if (hJob == IntPtr.Zero)
        {
            Console.WriteLine("Unable to create job object");
            return;
        }

        JOBOBJECT_EXTENDED_LIMIT_INFORMATION jobInfo = new JOBOBJECT_EXTENDED_LIMIT_INFORMATION();
        jobInfo.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_PROCESS_MEMORY | JOB_OBJECT_LIMIT_JOB_MEMORY;
        jobInfo.ProcessMemoryLimit = (UIntPtr)(512 * 1024 * 1024); // 512 MB
        jobInfo.JobMemoryLimit = (UIntPtr)(512 * 1024 * 1024); 

        int length = Marshal.SizeOf(typeof(JOBOBJECT_EXTENDED_LIMIT_INFORMATION));
        IntPtr jobInfoPtr = Marshal.AllocHGlobal(length);
        Marshal.StructureToPtr(jobInfo, jobInfoPtr, false);

        if (!SetInformationJobObject(hJob, JobObjectInfoType.JobObjectExtendedLimitInformation, jobInfoPtr, (uint)length))
        {
            Console.WriteLine("Unable to set job object information");
            return;
        }

        IntPtr hProcess = Process.GetCurrentProcess().Handle;
        if (!AssignProcessToJobObject(hJob, hProcess))
        {
            Console.WriteLine("Unable to assign process to job object");
            return;
        }


        string input = Path.Combine(Environment.CurrentDirectory, "input.txt");
        string output = Path.Combine(Environment.CurrentDirectory, "output.txt");
        long targetSizeInBytes = 1L * 1024 * 1024 * 1024; // 1 GB
        const int NumBlock = 20; 
        const int RunSizeMB = 50; 
        const long RunSizeBytes = RunSizeMB * 1024 * 1024;

       
        Console.WriteLine("Generating File...");
        try
        {
            using (var fs = new FileStream(input, FileMode.Create, FileAccess.Write))
            using (var writer = new StreamWriter(fs))
            {
                Random random = new Random();
                long totalLinesWritten = 0;
                long totalLinesToWrite = targetSizeInBytes / 10;

                while (totalLinesWritten < totalLinesToWrite)
                {
                    int randomNumber = random.Next(0, int.MaxValue);
                    writer.WriteLine(randomNumber);
                    totalLinesWritten++;
                }
            }

            Console.WriteLine("File generated successfully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred while generating a file: {ex.Message}");
        }



        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        Console.WriteLine("Sorting the file...");
        try
        {
            ExternalSort.ExternalSortFile(input, output, NumBlock, RunSizeBytes); 
            Console.WriteLine("Sorting completed.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred while sorting the file: {ex.Message}");
        }

        Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}");


        Marshal.FreeHGlobal(jobInfoPtr);
    }
}

