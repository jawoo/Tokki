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
    static int latBandSize = 10;   // degrees latitude per phenology band
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
            directoryFloweringDates = layout.tempFlowering();
            directoryInputPlatingDates = layout.tempPlantingDates();
            directoryError = layout.tempErrors();

            // Scenario switch
            boolean[] switches = cfg.switchScenarios();
            System.arraycopy(switches, 0, switchScenarios, 0, Math.min(switches.length, switchScenarios.length));

            // Fixed planting date and phenology stratification
            useFixedPlantingDate = cfg.useFixedPlantingDate();
            fixedPlantingDate = cfg.fixedPlantingDate();
            latBandSize = cfg.latBandSize();

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
                FileUtils.cleanDirectory(new File(directoryFloweringDates));
                FileUtils.cleanDirectory(new File(directoryInputPlatingDates));
                FileUtils.cleanDirectory(new File(directoryFinal));
            }

        } //if (step1)
    
        // CO2
        TreeMap<Integer, Integer> co2History = Utility.getCO2History(directoryInput);   

        // Get unit information
        Object[] unitInfo = Utility.getUnitInfo(tableNameUnitInformation, directoryInput, limitForDebugging);
        int numberOfUnits = unitInfo.length;
        System.out.println("> Number of units to run: "+numberOfUnits);
    

        /*
        2. FINDING PROMISING PLANTING DATES
        */
        System.out.println("> First planting year: "+firstPlantingYear);
        TreeMap<Object, Object> plantingDatesToSimulate = getPlantingDates(unitInfo);

        // Write planting-date debug CSV (CELL5M, X, Y, CropCode, Year, PlantingDOY)
        if (step2 && !plantingDatesToSimulate.isEmpty())
        {
            try
            {
                // Deduplicate units to one (CELL5M, cropCode) → (X, Y) entry
                // (multiple cultivars share the same planting date per cell+crop+year)
                TreeMap<String, double[]> cellCropToXY = new TreeMap<>();
                for (Object unitObj : unitInfo)
                {
                    Object[] o  = (Object[]) unitObj;
                    String   k  = o[1] + "_" + o[6];           // CELL5M_CROPCODE
                    cellCropToXY.putIfAbsent(k, new double[]{ (double) o[10], (double) o[11] });
                }

                String pdCsvPath = directoryInputPlatingDates + "plantingdates.csv";
                try (FileWriter pdWriter = new FileWriter(pdCsvPath);
                     CSVPrinter pdCsv = new CSVPrinter(pdWriter,
                             CSVFormat.DEFAULT.builder()
                                     .setHeader("CELL5M", "X", "Y", "CropCode", "Year", "PlantingDOY")
                                     .get()))
                {
                    for (Map.Entry<String, double[]> entry : cellCropToXY.entrySet())
                    {
                        String[] kp       = entry.getKey().split("_");
                        String   cell5m   = kp[0];
                        String   cropCode = kp[1];
                        double   x        = entry.getValue()[0];
                        double   y        = entry.getValue()[1];

                        for (int yr = firstPlantingYear; yr < firstPlantingYear + numberOfYears; yr++)
                        {
                            Object pdObj = plantingDatesToSimulate.get(cell5m + "_" + cropCode + "_" + yr);
                            if (pdObj != null)
                                pdCsv.printRecord(cell5m, x, y, cropCode, yr, pdObj);
                        }
                    }
                    pdCsv.flush();
                }
                System.out.println("> Planting dates written: " + pdCsvPath);
            }
            catch (Exception ex)
            {
                System.err.println("> Failed to write planting dates CSV (" + ex + ")");
            }
        }


        /*
        3. ADDITIONAL RUNS FOR RETRIEVING DAYS-TO-FLOWER FOR EACH VARIETY
        */
        System.out.println("> Retrieving days to flowering...");
        TreeMap<Object, Object> daysToFloweringByCultivar = new TreeMap<>();
        try
        {
            daysToFloweringByCultivar = getFloweringDates(unitInfo, plantingDatesToSimulate, co2History);
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
                ConsoleProgress plantingScanProgress = new ConsoleProgress("Planting date scan", numberOfUnits);

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

                    // Scanning planting dates (per year)
                    Future<Object[]> future = executor.submit(new ScanningPlantingDates(medianPlantingDate, weatherFileName, cropCode, firstPlantingYear, numberOfYears, plantingScanProgress));
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
                plantingScanProgress.finish();
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

    // Find flowering dates stratified by (cultivar, latitude band, simulation year).
    // Key format in daysToFloweringByCultivar: cropCode+cultivarCode+"_"+latBand+"_"+year
    // e.g. "MZIB0032_30_2021"
    @SuppressWarnings({"UseSpecificCatch", "CallToPrintStackTrace"})
    public static TreeMap<Object, Object> getFloweringDates(Object[] unitInfo,
                                                            TreeMap<Object, Object> plantingDatesToSimulate,
                                                            TreeMap<Integer, Integer> co2History) throws IOException {
        TreeMap<Object, Object> daysToFloweringByCultivar = new TreeMap<>();
        String plantingDateOptionLabel = "PB";
        int numberOfUnits = unitInfo.length;

        if (step3 && numberOfUnits > 0)
        {
            int threadID = 0;
            ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
            List<Future<Integer>> futures = new ArrayList<>();

            // Build a map of dtfKey → representative unit for every unique
            // (cropCode, cultivarCode, latBand, year) combination present in unitInfo.
            // One DSSAT flowering call is submitted per combination.
            TreeMap<String, Object[]> combinationToUnit = new TreeMap<>();
            for (Object unitObj : unitInfo)
            {
                Object[] o = (Object[]) unitObj;
                String cropCode    = (String) o[6];
                String cultivarCode = (String) o[7];
                double unitY       = (double) o[11];
                int latBand = (int)(Math.floor(unitY / latBandSize) * latBandSize);

                for (int yr = firstPlantingYear; yr < firstPlantingYear + numberOfYears; yr++)
                {
                    String dtfKey = cropCode + cultivarCode + "_" + latBand + "_" + yr;
                    if (!combinationToUnit.containsKey(dtfKey))
                    {
                        combinationToUnit.put(dtfKey, o);
                        daysToFloweringByCultivar.put(dtfKey, new int[]{0, 0});
                    }
                }
            }
            long floweringTotal = combinationToUnit.size();
            System.out.println("> Flowering: " + floweringTotal
                    + " unique (cultivar × lat-band × year) combinations to simulate.");

            ConsoleProgress floweringProgress = floweringTotal > 0
                    ? new ConsoleProgress("Flowering DSSAT", floweringTotal)
                    : null;

            // Submit one flowering run per combination
            for (Map.Entry<String, Object[]> entry : combinationToUnit.entrySet())
            {
                try
                {
                    String dtfKey  = entry.getKey();
                    Object[] o     = entry.getValue();

                    // Parse year and latBand from key: cropCultivar(8)_latBand_year
                    String[] kp  = dtfKey.split("_");
                    int simYear  = Integer.parseInt(kp[kp.length - 1]);
                    int latBand  = Integer.parseInt(kp[kp.length - 2]);

                    String cropCode     = (String) o[6];
                    String cultivarCode = (String) o[7];
                    String cultivarName = (String) o[8];
                    int[]  cultivarInfo = (int[])  o[9];
                    Object[] cultivarOption = new Object[]{ countryCode, cropCode, cultivarCode,
                            cultivarName, cultivarInfo[0], cultivarInfo[1], cultivarInfo[2] };

                    int cell5m = (Integer) o[1];
                    String weatherFileName = cell5m + ".WTH";
                    String weatherKey = cell5m + "_" + cropCode + "_" + simYear;

                    int pd = (int) o[5]; // median planting date as fallback
                    if (plantingDatesToSimulate.containsKey(weatherKey))
                    {
                        int looked = (Integer) plantingDatesToSimulate.get(weatherKey);
                        if (looked > 0) pd = looked;
                    }

                    int co2 = co2History.containsKey(simYear)
                            ? co2History.get(simYear)
                            : co2History.firstEntry().getValue();

                    Future<Integer> future = executor.submit(
                            new ThreadFloweringRuns(o, threadID, weatherFileName, pd,
                                    cultivarOption, plantingDateOptionLabel, co2, latBand, simYear, floweringProgress));
                    futures.add(future);
                    threadID++;
                    if (threadID == numberOfThreads) threadID = 0;
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

            if (floweringProgress != null)
                floweringProgress.finish();

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

                // Extract latBand from filename: find the part starting with "L" before the timestamp.
                // Filename format: {cell5m}_PB_{cropCode}_{cultivarCode}_Y{year}_L{latBand}_{timestamp}.csv
                int latBandFromFile = 0;
                for (String fp : csvFileName.replace(".csv", "").split("_"))
                {
                    if (fp.startsWith("L") && Utility.isNumeric(fp.substring(1)))
                    {
                        latBandFromFile = Integer.parseInt(fp.substring(1));
                        break;
                    }
                }
                final int parsedLatBand = latBandFromFile;

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

                            // Parse PDAT/ADAT/HDAT for computing flowering/harvest days.
                            // PDAT in summary.csv is YYYYDDD; extract year (chars 0-3) and DDD (chars 4-6).
                            int dtf, dth, pDDD = 0, aDDD = 0, hDDD = 0, pdatYear = 0;
                            String pdat = record.get("PDAT");
                            String adat = record.get("ADAT");
                            String hdat = record.get("HDAT");
                            if (pdat.length() >= 7)
                            {
                                pdatYear = Integer.parseInt(pdat.substring(0, 4));
                                pDDD = Integer.parseInt(pdat.substring(4));
                            }
                            if (adat.length() > 4) aDDD = Integer.parseInt(adat.substring(4));
                            if (hdat.length() > 4) hDDD = Integer.parseInt(hdat.substring(4));

                            // Full key: cropCultivar_latBand_year
                            String dtfKey = cropCode + cultivarCode + "_" + parsedLatBand + "_" + pdatYear;

                            if (pDDD > 0 && aDDD > 0 && hDDD > 0 && pdatYear > 0)
                            {
                                // Flowering days-after-planting
                                if (aDDD > pDDD)
                                    dtf = aDDD - pDDD + 1;
                                else
                                    dtf = aDDD + (365 - pDDD) + 1;

                                // Harvest days-after-planting
                                if (hDDD > pDDD)
                                    dth = hDDD - pDDD + 1;
                                else
                                    dth = hDDD + (365 - pDDD) + 1;

                                if (dtf > 0 && dth > 0 && dtf < dth)
                                {
                                    dtfMap.computeIfAbsent(dtfKey, k -> new ArrayList<>()).add(dtf);
                                    dthMap.computeIfAbsent(dtfKey, k -> new ArrayList<>()).add(dth);
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

            // Compute mean DTF and DTH for each full key, store in daysToFloweringByCultivar
            int temp;
            for (String dtfKey : dtfMap.keySet())
            {
                ArrayList<Integer> dtfValues = dtfMap.get(dtfKey);
                ArrayList<Integer> dthValues = dthMap.get(dtfKey);

                temp = 0;
                for (int v : dtfValues) temp += v;
                int dtfMean = temp / dtfValues.size();

                temp = 0;
                for (int v : dthValues) temp += v;
                int dthMean = temp / dthValues.size();

                daysToFloweringByCultivar.put(dtfKey, new int[]{dtfMean, dthMean});
            }

            // Global default fallback (60 DTF, 120 DTH) used when no key matches
            daysToFloweringByCultivar.put("DEFAULT", new int[]{ 60, 120 });

            // Replace any remaining zero-valued entries so the lookup table is complete
            fillMissingDtf(daysToFloweringByCultivar);

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

    private record SeasonalUnitWork(
            Object[] o,
            Object[] weatherAndPlantingDate,
            Object[] cultivarOption,
            int daysToFlowering,
            int daysToHarvest,
            TreeMap<Integer, int[]> yearToDtf)
    {
    }

    private static SeasonalUnitWork prepareSeasonalUnitWork(
            int i,
            Object[] unitInfo,
            TreeMap<Object, Object> plantingDatesToSimulate,
            TreeMap<Object, Object> daysToFloweringByCultivar,
            int firstPlantingYear,
            int numberOfYears,
            boolean logDefaultPlantingDate)
    {
        Object[] o = (Object[]) unitInfo[i];
        String cropCode = (String) o[6];
        String cultivarCode = (String) o[7];
        String cultivarName = (String) o[8];
        int[] cultivarInfo = (int[]) o[9];
        Object[] cultivarOption = new Object[]{ countryCode, cropCode, cultivarCode, cultivarName, cultivarInfo[0], cultivarInfo[1], cultivarInfo[2] };
        String cropCultivarCode = cultivarOption[1] + (String) cultivarOption[2];

        double unitLat = (double) o[11];
        int latBand = (int) (Math.floor(unitLat / latBandSize) * latBandSize);

        int[] fallbackDtf = (int[]) daysToFloweringByCultivar.get("DEFAULT");
        for (Object k : daysToFloweringByCultivar.keySet())
        {
            if (((String) k).startsWith(cropCultivarCode + "_"))
            {
                fallbackDtf = (int[]) daysToFloweringByCultivar.get(k);
                break;
            }
        }
        int daysToFlowering = fallbackDtf[0];
        int daysToHarvest = fallbackDtf[1];

        TreeMap<Integer, int[]> yearToDtf = new TreeMap<>();
        for (int yr = firstPlantingYear; yr < firstPlantingYear + numberOfYears; yr++)
        {
            String dtfKey = cropCultivarCode + "_" + latBand + "_" + yr;
            if (daysToFloweringByCultivar.containsKey(dtfKey))
                yearToDtf.put(yr, (int[]) daysToFloweringByCultivar.get(dtfKey));
        }

        String cell5m = String.valueOf((Integer) o[1]);
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
                    p = (int) (plantingDatesToSimulate.get(weatherKey));
                }
                catch (Exception e)
                {
                    if (logDefaultPlantingDate)
                        System.out.println("> Default planting date used for " + cropCultivarCode + " year " + yr);
                }
            }
            yearToPlantingDate.put(yr, p);
        }
        Object[] weatherAndPlantingDate = { weatherFileName, yearToPlantingDate };
        return new SeasonalUnitWork(o, weatherAndPlantingDate, cultivarOption, daysToFlowering, daysToHarvest, yearToDtf);
    }

    // Run seasonal simulations
    public static void runSeasonalSimulations(Object[] unitInfo, 
                                              TreeMap<Object, Object> plantingDatesToSimulate,
                                              TreeMap<Object, Object> daysToFloweringByCultivar,
                                              int firstPlantingYear, int numberOfYears, TreeMap<Integer, Integer> co2History)
            throws ExecutionException, InterruptedException
    {
        int numberOfUnits = unitInfo.length;

        long totalDssatRuns = 0;
        if (step4)
        {
            for (int i = 0; i < numberOfUnits; i++)
            {
                SeasonalUnitWork w = prepareSeasonalUnitWork(i, unitInfo, plantingDatesToSimulate, daysToFloweringByCultivar, firstPlantingYear, numberOfYears, false);
                totalDssatRuns += ThreadSeasonalRuns.countDssatInvocations(w.weatherAndPlantingDate(), w.cultivarOption(), co2History);
            }
        }

        ConsoleProgress seasonalProgress = (step4 && totalDssatRuns > 0)
                ? new ConsoleProgress("Seasonal DSSAT", totalDssatRuns)
                : null;

        // Multithreading
        ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder().setNameFormat("%d");
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads, threadFactoryBuilder.build());
        List<Future<Integer>> futures = new ArrayList<>();

        for (int i = 0; i < numberOfUnits; i++)
        {
            if (step4)
            {
                try
                {
                    SeasonalUnitWork w = prepareSeasonalUnitWork(i, unitInfo, plantingDatesToSimulate, daysToFloweringByCultivar, firstPlantingYear, numberOfYears, true);
                    Future<Integer> future = executor.submit(new ThreadSeasonalRuns(
                            w.o(),
                            w.weatherAndPlantingDate(),
                            w.cultivarOption(),
                            w.daysToFlowering(),
                            w.daysToHarvest(),
                            w.yearToDtf(),
                            firstPlantingYear,
                            numberOfYears,
                            co2History,
                            seasonalProgress));
                    futures.add(future);
                }
                catch (Exception ex)
                {
                    System.err.println("> Seasonal runs: failed to submit task for R" + (i + 1) + "/" + numberOfUnits + " (" + ex + ")");
                }
            }
        }

        // Retrieve
        for (Future<Integer> future: futures)
        {
            future.get();
        }

        if (seasonalProgress != null)
            seasonalProgress.finish();

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

    /**
     * Fills zero-valued DTF/DTH entries in {@code dtfMap} so the lookup table
     * contains no zeros after simulation failures.
     *
     * Key format:  cropCultivar(8 chars) + "_" + latBand + "_" + year
     *              e.g. "MZIB0032_30_2021"
     * Special key: "DEFAULT" (excluded from fill logic).
     *
     * Pass 1 – Missing years within a (cultivar, latBand) group:
     *   Uses the mean DTF/DTH of whichever years in the same group succeeded.
     *
     * Pass 2 – Groups where every year failed:
     *   Searches adjacent latitude bands (±latBandSize, ±2*latBandSize, …) and
     *   uses the mean of any valid values found at the nearest offset.
     *   Falls back to DEFAULT only if no adjacent band has any data at all.
     */
    private static void fillMissingDtf(TreeMap<Object, Object> dtfMap)
    {
        // Retrieve the last-resort default (always present at this call site)
        Object defObj = dtfMap.get("DEFAULT");
        int[] defaultVal = (defObj != null) ? (int[]) defObj : new int[]{ 60, 120 };

        // ── Index all non-DEFAULT keys by their (cultivar, latBand) group ──────
        // Group key: cropCultivar + "_" + latBand  (e.g. "MZIB0032_30")
        TreeMap<String, List<String>> groupToYearKeys = new TreeMap<>();
        for (Object keyObj : dtfMap.keySet())
        {
            String key = (String) keyObj;
            if (key.equals("DEFAULT")) continue;
            String[] parts = key.split("_");
            if (parts.length != 3) continue;                          // safety guard
            String groupKey = parts[0] + "_" + parts[1];
            groupToYearKeys.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(key);
        }

        // ── Pass 1: fill individual failed years with same-group mean ─────────
        Set<String> allFailedGroups = new HashSet<>();

        for (Map.Entry<String, List<String>> entry : groupToYearKeys.entrySet())
        {
            String       groupKey = entry.getKey();
            List<String> yearKeys = entry.getValue();

            List<int[]>  good = new ArrayList<>();
            List<String> bad  = new ArrayList<>();
            for (String yk : yearKeys)
            {
                int[] v = (int[]) dtfMap.get(yk);
                if (v != null && v[0] > 0 && v[1] > 0) good.add(v);
                else                                     bad.add(yk);
            }

            if (bad.isEmpty()) continue;            // every year succeeded

            if (!good.isEmpty())
            {
                int dtfSum = 0, dthSum = 0;
                for (int[] v : good) { dtfSum += v[0]; dthSum += v[1]; }
                int[] fill = { dtfSum / good.size(), dthSum / good.size() };
                for (String yk : bad)
                {
                    dtfMap.put(yk, fill);
                    System.out.println("> DTF fill (same-band mean): " + yk
                            + " → DTF=" + fill[0] + " DTH=" + fill[1]);
                }
            }
            else
            {
                allFailedGroups.add(groupKey);      // all years failed; needs Pass 2
            }
        }

        // ── Pass 2: fill completely-failed groups from adjacent lat bands ──────
        // Collect updates in a separate map to avoid ConcurrentModificationException.
        TreeMap<String, int[]> updates = new TreeMap<>();

        for (String failedGroup : allFailedGroups)
        {
            String[] gp       = failedGroup.split("_");
            String   cultivar = gp[0];
            int      latBand  = Integer.parseInt(gp[1]);

            int[] fill = null;

            // Expand outward one band at a time; stop at the first offset that
            // yields any valid data from either the band above or below.
            for (int offset = 1; fill == null && offset <= 20; offset++)
            {
                List<int[]> candidates = new ArrayList<>();
                int[] adjBands = { latBand + offset * latBandSize,
                                   latBand - offset * latBandSize };

                for (int adjBand : adjBands)
                {
                    String adjGroup = cultivar + "_" + adjBand;
                    if (allFailedGroups.contains(adjGroup)) continue;   // also all-failed
                    List<String> adjKeys = groupToYearKeys.get(adjGroup);
                    if (adjKeys == null) continue;                      // band not in dataset
                    for (String yk : adjKeys)
                    {
                        int[] v = (int[]) dtfMap.get(yk);
                        // After Pass 1 every non-failed-group key is non-zero
                        if (v != null && v[0] > 0 && v[1] > 0) candidates.add(v);
                    }
                }

                if (!candidates.isEmpty())
                {
                    int dtfSum = 0, dthSum = 0;
                    for (int[] v : candidates) { dtfSum += v[0]; dthSum += v[1]; }
                    fill = new int[]{ dtfSum / candidates.size(), dthSum / candidates.size() };
                }
            }

            if (fill == null)
            {
                fill = defaultVal;                  // absolute last resort
                System.out.println("> DTF fill (DEFAULT): " + failedGroup);
            }

            List<String> yearKeys = groupToYearKeys.get(failedGroup);
            if (yearKeys != null)
            {
                for (String yk : yearKeys)
                {
                    updates.put(yk, fill);
                    System.out.println("> DTF fill (adj-band mean): " + yk
                            + " → DTF=" + fill[0] + " DTH=" + fill[1]);
                }
            }
        }

        dtfMap.putAll(updates);
    }



}
