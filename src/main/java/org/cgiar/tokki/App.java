package org.cgiar.tokki;

// Java utilities
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

// CSV and file utilities
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;

// Google utilities
import com.google.common.util.concurrent.ThreadFactoryBuilder;

// Main class
public class App 
{

    // Some initialization and settings
    static long timeInitial = System.currentTimeMillis();
    static String tableNameUnitInformation = "";
    static String d = File.separator;
    static String fileDaysToFlowering = "daystoflowering";
    static String directoryWeather;
    static String directoryMultiplePlatingDates;
    static String directoryFloweringDates;
    static String directoryInputPlatingDates;
    static String directoryInput;
    static String directoryOutput;
    static String directoryFinal;
    static String directoryError;
    static String directorySource;
    static String directoryThreads;
    static String dataDaysToFlowering;
    static int numberOfThreads = 4;    // per box
    static String countryCode = "ETH";
    static int limitForDebugging = 1;
    static int firstPlantingYear = 2021;
    static int numberOfYears = 5;
    static boolean useAvgPlantingDensity = true;
    static boolean verbose = false;
    static boolean cleanUpFirst = true;
    static int numberOfCultivars = 0;
    static boolean printDaysToFlowering = true;
    static boolean step1 = true;   // preparation
    static boolean step2 = true;   // testingMultiplePlantingDates
    static boolean step3 = true;   // retrievingDaysToFlowering
    static boolean step4 = true;   // seasonalRuns
    static boolean step5 = true;   // wrappingUp

    // Scenario switch
    /*
    0: Water Management
    1: Fertilizer
    2: Manure
    3: Residue
    4: Planting Window
    5: Planting Density
    6: CO2 Fertilization
    */
    static boolean[] switchScenarios = new boolean[7];
    static boolean scenarioCombinations = true;
    static boolean useRecommendedNitrogenFertilizerRate = true;
    static boolean useFixedPlantingDate = false;
    static int fixedPlantingDate = 135;
    static Object[] nitrogenFertilizerRates;
    static Object[] atmosphericCO2Values;
    static String nr = System.lineSeparator();

    // Main method
    @SuppressWarnings({"CallToPrintStackTrace", "UseSpecificCatch"})
    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException
    {

        /*
        0. SETTING PARAMETERS
        */
        try
        {
            TokkiConfig cfg = ConfigLoader.load("." + d + "config.yml", d);
            tableNameUnitInformation = cfg.tableNameUnitInformation();
            countryCode = cfg.countryCode();
            numberOfThreads = cfg.numberOfThreads();
            limitForDebugging = cfg.limitForDebugging();
            firstPlantingYear = cfg.firstPlantingYear();
            numberOfYears = cfg.numberOfYears();
            scenarioCombinations = cfg.scenarioCombinations();
            useRecommendedNitrogenFertilizerRate = cfg.useRecommendedNitrogenFertilizerRate();
            nitrogenFertilizerRates = cfg.nitrogenFertilizerRates();
            atmosphericCO2Values = cfg.atmosphericCO2Values();

            DirectoryLayout layout = cfg.directories();
            layout.working();
            directoryWeather = layout.weather();
            directorySource = layout.source();
            directoryInput = layout.input();
            directoryThreads = layout.threads();
            directoryFinal = layout.result();
            directoryOutput = layout.outputSummary();
            directoryMultiplePlatingDates = layout.tempPlanting();
            directoryFloweringDates = layout.tempFlowering();
            directoryInputPlatingDates = layout.tempPlantingDates();
            directoryError = layout.tempErrors();

            // Scenario switch
            cfg.dataPlantingDates();
            boolean[] switches = cfg.switchScenarios();
            System.arraycopy(switches, 0, switchScenarios, 0, Math.min(switches.length, switchScenarios.length));

            // Fixed planting date
            useFixedPlantingDate = cfg.useFixedPlantingDate();
            fixedPlantingDate = cfg.fixedPlantingDate();

        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }

        /*
        1. PREPARATION
        */
        if (step1)
        {

            // Showing some parameter values
            System.out.println("> OS: "+Utility.OS.toUpperCase());
            System.out.println("> ISO3: "+countryCode);
            System.out.println("> Number of threads: "+numberOfThreads);
            System.out.println("> Limit: "+limitForDebugging);
            System.out.println("> Weather data: "+directoryWeather);
            System.out.println("> Management practice - Water: "+(switchScenarios[0] ? "ON" : "OFF"));
            System.out.println("> Management practice - Fertilizer: "+(switchScenarios[1] ? "ON" : "OFF")+" ("+Utility.getString(nitrogenFertilizerRates)+")");
            System.out.println("> Management practice - Manure: "+(switchScenarios[2] ? "ON" : "OFF"));
            System.out.println("> Management practice - Residue: "+(switchScenarios[3] ? "ON" : "OFF"));
            System.out.println("> Management practice - Planting window: "+(switchScenarios[4] ? "ON" : "OFF"));
            System.out.println("> Management practice - Planting density: "+(switchScenarios[5] ? "ON" : "OFF"));
            System.out.println("> Management practice - CO2 fertilization: "+(switchScenarios[6] ? "ON" : "OFF")+" ("+Utility.getString(atmosphericCO2Values)+")");
            System.out.println("> Management practice - Factorial combinations: "+(scenarioCombinations ? "ON" : "OFF"));

            // Copying Toucan workspace files
            File toucanSource = new File(directorySource);
            try
            {

                // Making T copies
                for(int t=0; t<numberOfThreads; t++)
                {
                    String dT = directoryThreads+"T"+t;
                    File toucanDestination = new File(dT);
                    FileUtils.copyDirectory(toucanSource, toucanDestination);

                    // File permission change if Linux
                    if (Utility.isUnix())
                    {

                        // Permission 777
                        Set<PosixFilePermission> perms = new HashSet<>();
                        perms.add(PosixFilePermission.OWNER_READ);
                        perms.add(PosixFilePermission.OWNER_WRITE);
                        perms.add(PosixFilePermission.OWNER_EXECUTE);
                        perms.add(PosixFilePermission.GROUP_READ);
                        perms.add(PosixFilePermission.GROUP_WRITE);
                        perms.add(PosixFilePermission.GROUP_EXECUTE);
                        perms.add(PosixFilePermission.OTHERS_READ);
                        perms.add(PosixFilePermission.OTHERS_WRITE);
                        perms.add(PosixFilePermission.OTHERS_EXECUTE);

                        // Apply to the directory
                        Files.setPosixFilePermissions(Paths.get(dT), perms);

                        // Apply to all files
                        String[] workingFileNames = Utility.getFileNames(dT);
                        for (String workingFileName : workingFileNames)
                            Files.setPosixFilePermissions(Paths.get(dT + d + workingFileName), perms);

                    }

                }

            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            // Delete files from previous runs
            if (cleanUpFirst)
            {
                FileUtils.cleanDirectory(new File(directoryOutput));
                FileUtils.cleanDirectory(new File(directoryError));
                FileUtils.cleanDirectory(new File(directoryMultiplePlatingDates));
                FileUtils.cleanDirectory(new File(directoryFloweringDates));
                FileUtils.cleanDirectory(new File(directoryInputPlatingDates));
                FileUtils.cleanDirectory(new File(directoryFinal));
            }

        } //if (step1)
    
        // CO2
        TreeMap<Integer, Integer> co2History = Utility.getCO2History(directoryInput);   
        int co2 = co2History.get(firstPlantingYear);

        // Get unit information
        Object[] unitInfo = Utility.getUnitInfo(tableNameUnitInformation, directoryInput, limitForDebugging);
        int numberOfUnits = unitInfo.length;
        System.out.println("> Number of units to run: "+numberOfUnits);
    

        /*
        2. FINDING PROMISING PLANTING DATES
        */
        System.out.println("> First planting year: "+firstPlantingYear);
        TreeMap<Object, Object> plantingDatesToSimulate = getPlantingDates(unitInfo);


        /*
        3. ADDITIONAL RUNS FOR RETRIEVING DAYS-TO-FLOWER FOR EACH VARIETY
        */
        System.out.println("> Retrieving days to flowering...");
        TreeMap<Object, Object> daysToFloweringByCultivar = new TreeMap<>();
        try
        {
            daysToFloweringByCultivar = getFloweringDates(unitInfo, plantingDatesToSimulate, firstPlantingYear, co2);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }


        /*
        4. SEASONAL RUNS
        */
        System.out.println("> Running seasonal simulations...");
        runSeasonalSimulations(unitInfo, plantingDatesToSimulate, daysToFloweringByCultivar, firstPlantingYear, numberOfYears, co2History);

                
        /*
        5. WRAPPING UP
        */
        if (step5)
        {
            boolean firstFile = true;
            String[] outputFileNames = Utility.getFileNames(directoryOutput);
            Date date = new Date();
            long timeStamp = date.getTime();

            // Write
            try
            {
                String combinedOutput = directoryFinal+"tokki_combinedOutput_"+timeStamp+".csv";
                
                // Looping through the files
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(combinedOutput))) {
                
                    // Looping through the files
                    String header;
                    for (String outputFileName: outputFileNames)
                    {
                        try (BufferedReader reader = new BufferedReader(new FileReader(directoryOutput+outputFileName)))
                        {
                            String line;

                            // To skip the header from the second file
                            if (firstFile)
                            {
                                header = reader.readLine()
                                        .replace("SOIL_ID","SoilProfileID")
                                        .replace("LATI","LAT")
                                        .replace("CR","CropCode")
                                        .replace("FNAM","CultivarCode");     // Replace SOIL_ID with SoilProfileID
                                writer.append(header).append(nr);
                                firstFile = false;
                            }

                            // Reader --> Writer
                            while ((line = reader.readLine()) != null)
                            {
                                String firstValue = line.split(",")[0];
                                if (Utility.isNumeric(firstValue))
                                    writer.append(line.replace("|", ",")).append(nr);
                            }
                        }
                    }
                }
                System.out.println("> Output files merged: "+combinedOutput);
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }

            // Delete temporary files for the next batch of runs
            try
            {
                Utility.deleteSummaryFiles();
            }
            catch (Exception ex)
            {
                System.out.println("> Failed to delete summary files");
            }

        } //if (step5)

            
        // How long did it take?
        long runningTime = (System.currentTimeMillis() - timeInitial)/(long)1000;
        String rt = String.format("%1$02d:%2$02d:%3$02d", runningTime / (60*60), (runningTime / 60) % 60, runningTime % 60);
        System.out.println("> Done ("+rt+")");
            
    }

    // Find promising planting dates
    @SuppressWarnings({"UseSpecificCatch", "CallToPrintStackTrace"})
    public static TreeMap<Object, Object> getPlantingDates(Object[] unitInfo)
    {
        TreeMap<Object, Object> plantingDatesToSimulate = new TreeMap<>();
        int numberOfUnits = unitInfo.length;

        // If step2 is false, an empty treemap will be returned.
        if (step2 && numberOfUnits>0)
        {
            try
            {
                ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
                List<Future<Object[]>> list = new ArrayList<>();

                // Execute
                for (int i=0; i<numberOfUnits; i++)
                {
                    Object[] o = (Object[])unitInfo[i];
                    int cell5m = (Integer) o[1];
                    String weatherFileName = String.valueOf(cell5m) + ".WTH";
                    int medianPlantingDate = (Integer) o[5];
                    String cropCode = (String) o[6];
                    System.out.println("> Planting date "+(i+1)+"/"+numberOfUnits+", CELL5M: "+cell5m+" for "+cropCode);

                    // Scanning planting dates (per year)
                    Future<Object[]> future = executor.submit(new ScanningPlantingDates(medianPlantingDate, weatherFileName, cropCode, firstPlantingYear, numberOfYears));
                    list.add(future);
                }

                // Retrieve — unpack year-keyed map into plantingDatesToSimulate
                // Key format: "CELL5M_CROPCODE_YEAR" (e.g. "2126127_MZ_2021")
                for (Future<Object[]> future: list)
                {
                    Object[] r = future.get();
                    String keyPrefix = (String) r[0];
                    @SuppressWarnings("unchecked")
                    TreeMap<Integer, Integer> yearToDate = (TreeMap<Integer, Integer>) r[1];
                    for (Map.Entry<Integer, Integer> entry : yearToDate.entrySet())
                    {
                        int plantingDate = useFixedPlantingDate ? fixedPlantingDate : entry.getValue();
                        plantingDatesToSimulate.put(keyPrefix + "_" + entry.getKey(), plantingDate);
                    }
                }
                executor.shutdown();

            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }
        }
        System.out.println("> Runs for scanning planting dates done. Found "+plantingDatesToSimulate.size()+" planting dates.");
        return plantingDatesToSimulate;
    }

    // Find average flowering dates
    @SuppressWarnings({"UseSpecificCatch", "CallToPrintStackTrace"})
    public static TreeMap<Object, Object> getFloweringDates(Object[] unitInfo, 
                                                            TreeMap<Object, Object> plantingDatesToSimulate,
                                                            int firstPlantingYear, int co2) throws IOException {
        TreeMap<Object, Object> daysToFloweringByCultivar = new TreeMap<>();
        String plantingDateOptionLabel = "PB";
        int numberOfUnits = unitInfo.length;

        // Distribute weather files over threads
        if (step3 && numberOfUnits>0)
        {
            int threadID = 0;
            ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
            List<Future<Integer>> futures = new ArrayList<>();

            // Let's just pick 200 random units
            int[] subUnits = new int[numberOfUnits];
            if (numberOfUnits>20)
            {
                subUnits = new int[200];
                Random random = new Random();
                for (int i = 0; i < subUnits.length; i++)
                    subUnits[i] = random.nextInt(numberOfUnits);
            }
            else
                for (int i = 0; i < numberOfUnits; i++)
                    subUnits[i] = i;

            // Looping through subUnits
            for (int u: subUnits)
            {
                try
                {
                    Object[] o = (Object[])unitInfo[u];
                    int pdMean = (int)o[5];

                    // Construct the cultivar option
                    String cropCode = (String)o[6];
                    String cultivarCode = (String)o[7];
                    String cultivarName = (String)o[8];
                    int[] cultivarInfo = (int[])o[9];
                    Object[] cultivarOption = new Object[]{ countryCode, cropCode, cultivarCode, cultivarName, cultivarInfo[0], cultivarInfo[1], cultivarInfo[2] };

                    // Weather name — key includes year since plantingDatesToSimulate is now year-keyed
                    int cell5m = (Integer)o[1];
                    String weatherFileName = String.valueOf(cell5m) + ".WTH";
                    String weatherKey = weatherFileName.split("\\.")[0] + "_" + cropCode + "_" + firstPlantingYear;

                    // To use below
                    daysToFloweringByCultivar.put(cropCode + cultivarCode, new int[]{0, 0});

                    // Planting dates to use
                    if (plantingDatesToSimulate.containsKey(weatherKey))
                    {
                        int pd = (Integer) plantingDatesToSimulate.get(weatherKey);
                        if (pd <= 0) pd = pdMean;
                        int finalThreadID = threadID;
                        int finalPd = pd;

                        // Multithreading
                        Future<Integer> future = executor.submit(new ThreadFloweringRuns(o, finalThreadID, weatherFileName, finalPd, cultivarOption, plantingDateOptionLabel, co2, firstPlantingYear));
                        futures.add(future);
                        threadID++;

                        if (threadID==numberOfThreads) threadID = 0;
                    }

                }
                catch (Exception ex)
                {
                    ex.printStackTrace();
                }
            }

            // Execute and retrieve the exit code
            for (Future<Integer> future: futures)
            {
                try
                {
                    int exitCode = future.get();
                    if (exitCode>0) System.out.println("> Failed to find good flowering dates...");
                }
                catch(InterruptedException | ExecutionException | NumberFormatException e)
                {
                    e.printStackTrace();
                }
            }

            // Shutdown the executor
            executor.shutdown();
            try
            {
                // Wait for all tasks to complete before continuing.
                if (!executor.awaitTermination(60, TimeUnit.SECONDS))
                {
                    // Cancel currently executing tasks
                    executor.shutdownNow();
                }
            }
            catch (InterruptedException ex)
            {
                // Cancel if current thread also interrupted
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Collect output files
            String[] csvFileNames = Utility.getFileNames(directoryFloweringDates, "CSV");

            // For each CSV file
            TreeMap<String, ArrayList<Integer>> dtfMap = new TreeMap<>();
            TreeMap<String, ArrayList<Integer>> dthMap = new TreeMap<>();
            for (String csvFileName : csvFileNames)
            {

                // Status
                System.out.println("> Analyzing " + csvFileName + "...");

                // Reading in
                try (var parser = CsvIO.openRfc4180(directoryFloweringDates + csvFileName))
                {
                    for (CSVRecord record : parser)
                    {
                        try
                        {
                            
                            // Parse the cultivar code from TNAM
                            String cropCode = record.get("CR");
                            String cultivarCode = record.get("TNAM").substring(0, 6);
                            String cropCultivarCode = cropCode + cultivarCode;

                            // Parse PDAT/ADAT/HDAT for computing flowering/harvest days
                            int dtf, dth, pDDD = 0, aDDD = 0, hDDD = 0;
                            String pdat = record.get("PDAT");
                            String adat = record.get("ADAT");
                            String hdat = record.get("HDAT");
                            if (pdat.length() > 4)
                                pDDD = Integer.parseInt(pdat.substring(4));
                            if (record.get("ADAT").length() > 4)
                                aDDD = Integer.parseInt(adat.substring(4));
                            if (record.get("HDAT").length() > 4)
                                hDDD = Integer.parseInt(hdat.substring(4));

                            if (pDDD > 0 && aDDD > 0 && hDDD > 0)
                            {
                                // Flowering date
                                if (aDDD > pDDD)
                                    dtf = aDDD - pDDD + 1;
                                else
                                    dtf = aDDD + (365 - pDDD) + 1;

                                // Harvest date
                                if (hDDD > pDDD)
                                    dth = hDDD - pDDD + 1;
                                else
                                    dth = hDDD + (365 - pDDD) + 1;

                                if (dtf > 0 && dth > 0 && dtf < dth)
                                {
                                    if (dtfMap.containsKey(cropCultivarCode))
                                    {
                                        // DTF
                                        ArrayList<Integer> dtfValues = dtfMap.get(cropCultivarCode);
                                        dtfValues.add(dtf);
                                        dtfMap.put(cropCultivarCode, dtfValues);

                                        // DTH
                                        ArrayList<Integer> dthValues = dthMap.get(cropCultivarCode);
                                        dthValues.add(dth);
                                        dthMap.put(cropCultivarCode, dthValues);
                                    }
                                    else
                                    {
                                        // DTF
                                        ArrayList<Integer> dtfValues = new ArrayList<>();
                                        dtfValues.add(dtf);
                                        dtfMap.put(cropCultivarCode, dtfValues);

                                        // DTH
                                        ArrayList<Integer> dthValues = new ArrayList<>();
                                        dthValues.add(dth);
                                        dthMap.put(cropCultivarCode, dthValues);
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            System.out.println("> Parsing error: " + csvFileName);
                        }
                    }
                } // For each row in the CSV file

            } // For each CSV file

            // List of unique cropCultivarCode
            ArrayList<String> cropCultivarList = new ArrayList<>(dtfMap.keySet());
            int temp;
            for (String c : cropCultivarList)
            {
                ArrayList<Integer> dtfValues = dtfMap.get(c);
                ArrayList<Integer> dthValues = dthMap.get(c);

                // Mean of DTF values
                temp = 0;
                for (Integer dtfValue : dtfValues) temp = temp + dtfValue;
                int dtfMean = temp / dtfValues.size();

                // Mean of DTH values
                temp = 0;
                for (Integer dthValue : dthValues) temp = temp + dthValue;
                int dthMean = temp / dthValues.size();

                // Storing
                daysToFloweringByCultivar.put(c, new int[]{dtfMean, dthMean});
            }

            // Adding some default values to avoid errors
            int dtfAvg = 60, dthAvg = 120;
            daysToFloweringByCultivar.put("DEFAULT", new int[]{ dtfAvg, dthAvg });

            // Writing a CSV output file
            if (printDaysToFlowering)
            {
                dataDaysToFlowering = directoryFloweringDates + fileDaysToFlowering + ".csv";
                System.out.println("> Writing " + dataDaysToFlowering + "...");
                try (FileWriter writer = new FileWriter(dataDaysToFlowering);
                     CSVPrinter csvPrinter = new CSVPrinter(
                             writer,
                             CSVFormat.DEFAULT.builder()
                                     .setHeader("CultivarCode", "AvgDaysToFlowering", "AvgDaysToHarvest")
                                     .get()))
                {

                    // Writing
                    for (Map.Entry<Object, Object> entry : daysToFloweringByCultivar.entrySet())
                    {
                        String key = (String) entry.getKey();
                        int[] value = (int[]) entry.getValue();
                        csvPrinter.printRecord(key, value[0], value[1]);
                    }
                    csvPrinter.flush();
                }
                catch (Exception ex)
                {
                    ex.printStackTrace();
                }
            }

        }
        return daysToFloweringByCultivar;
    }

        // Run seasonal simulations
    public static void runSeasonalSimulations(Object[] unitInfo, 
                                              TreeMap<Object, Object> plantingDatesToSimulate,
                                              TreeMap<Object, Object> daysToFloweringByCultivar,
                                              int firstPlantingYear, int numberOfYears, TreeMap<Integer, Integer> co2History)
            throws ExecutionException, InterruptedException
    {
        int numberOfUnits = unitInfo.length;

        // Multithreading
        ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder().setNameFormat("%d");
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads, threadFactoryBuilder.build());
        List<Future<Integer>> futures = new ArrayList<>();

        // Looping through units
        for (int i = 0; i < numberOfUnits; i = i + 1)
        {

            // Status
            String progress = "R" + (i+1) + "/" + numberOfUnits;

            // Subset
            if (step4)
            {
                try
                {
                    if (i < numberOfUnits)
                    {

                        // Unit Information
                        Object[] o = (Object[]) unitInfo[i];

                        // Construct the cultivar option
                        String cropCode = (String)o[6];
                        String cultivarCode = (String)o[7];
                        String cultivarName = (String)o[8];
                        int[] cultivarInfo = (int[])o[9];
                        Object[] cultivarOption = new Object[]{ countryCode, cropCode, cultivarCode, cultivarName, cultivarInfo[0], cultivarInfo[1], cultivarInfo[2] };
                        String cropCultivarCode = cultivarOption[1] + (String)cultivarOption[2];
                        int daysToFlowering, daysToHarvest;
                        try
                        {
                            daysToFlowering = ((int[])daysToFloweringByCultivar.get(cropCultivarCode))[0];
                            daysToHarvest = ((int[])daysToFloweringByCultivar.get(cropCultivarCode))[1];
                        }
                        catch(Exception e)
                        {
                            daysToFlowering = ((int[])daysToFloweringByCultivar.get("DEFAULT"))[0];
                            daysToHarvest = ((int[])daysToFloweringByCultivar.get("DEFAULT"))[1];
                            System.out.println("> Default phenology values used for "+cropCultivarCode);
                        }

                        // Weather file and per-year planting dates
                        String cell5m = String.valueOf((Integer)o[1]);
                        String weatherFileName = cell5m + ".WTH";
                        String keyPrefix = cell5m + "_" + cropCode;
                        TreeMap<Integer, Integer> yearToPlantingDate = new TreeMap<>();
                        for (int yr = firstPlantingYear; yr < firstPlantingYear + numberOfYears; yr++)
                        {
                            int p = fixedPlantingDate;
                            if (!useFixedPlantingDate)
                            {
                                String weatherKey = keyPrefix + "_" + yr;
                                try
                                {
                                    p = (int)(plantingDatesToSimulate.get(weatherKey));
                                }
                                catch (Exception e)
                                {
                                    System.out.println("> Default planting date used for " + cropCultivarCode + " year " + yr);
                                }
                            }
                            yearToPlantingDate.put(yr, p);
                        }
                        Object[] weatherAndPlantingDate = { weatherFileName, yearToPlantingDate };

                        // Multiple threads
                        Future<Integer> future = executor.submit(new ThreadSeasonalRuns(o, weatherAndPlantingDate, cultivarOption, daysToFlowering, daysToHarvest, progress, firstPlantingYear, numberOfYears, co2History));
                        futures.add(future);

                    }
                }
                catch (Exception ex)
                {
                    System.err.println("> Seasonal runs: failed to submit task for " + progress + " (" + ex + ")");
                }

            }

        } // Looping through units

        // Retrieve
        for (Future<Integer> future: futures)
        {
            future.get();
        }

        // Shutdown the executor
        executor.shutdown();
        try
        {
            // Wait for all tasks to complete before continuing.
            if (!executor.awaitTermination(60, TimeUnit.SECONDS))
            {
                // Cancel currently executing tasks
                executor.shutdownNow();
            }
        }
        catch (InterruptedException ex)
        {
            // Cancel if current thread also interrupted
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

    }


    
}
