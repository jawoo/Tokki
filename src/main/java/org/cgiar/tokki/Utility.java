package org.cgiar.tokki;

// Java utilities
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.regex.Pattern;

// Apache utilities
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

// Google utilities
import com.google.common.collect.Lists;

// SnakeYAML utilities (JSONL parsing)
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

// Utility class
public class Utility 
{
    static Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");
    static String OS = System.getProperty("os.name").toLowerCase();

    // Delay before retry
    private static void delayBeforeRetry(int millis) throws InterruptedException 
    {
        Thread.sleep(millis);
    }

    // Rooting depth adjustment
    static String updateSoilProfileDepth(String soilProfile, int slbMax)
    {
        DecimalFormat dfDDD = new DecimalFormat("000");
        String crlf	= System.getProperty("line.separator");
        StringBuilder soilProfileModified = new StringBuilder();
        String[] soilProfileByLine = soilProfile.split(crlf);

        // Locating Tier 1 and Tier 2 layers
        int lineNumberHeadingTier2 = 0;
        for (int s=6; s<soilProfileByLine.length; s++)
            if (soilProfileByLine[s].substring(0, 1).equals("@"))
                lineNumberHeadingTier2 = s;

        // Setting the minimum depth as 40 cm, which is the median value of SLB_MAX for SSA for the shallow soils (0-90 cm; see HC.SOL)
        slbMax = Math.max(slbMax, 40);

        // Copying header lines
        boolean slbMaxFound = false;
        for (int s=0; s<6; s++)
            soilProfileModified.append(soilProfileByLine[s]).append(crlf);

        // Tier 2 is not needed
        int sMax = soilProfileByLine.length;
        if (lineNumberHeadingTier2>0)
            sMax = lineNumberHeadingTier2;

        for (int s=6; s<sMax; s++)
        {
            if (!slbMaxFound)
            {
                String d = soilProfileByLine[s].substring(3,6).trim();
                if (isNumeric(d))
                {
                    int slb = Integer.parseInt(d);
                    if (slb<slbMax)
                        soilProfileModified.append(soilProfileByLine[s]).append(crlf);
                    else
                    {
                        soilProfileModified.append("   ").append(dfDDD.format(slbMax)).append(soilProfileByLine[s].substring(6)).append(crlf);
                        slbMaxFound = true;
                    }
                }
            }
        }
        return soilProfileModified.toString();
    }

    // Copying files using Stream
    static void copyFileUsingStream(File source, File dest) throws InterruptedException, IOException
    {
        int maxTries = 3;
        for (int count = 0; count < maxTries; count++)
        {
            try
            {
                var parent = dest.toPath().getParent();
                if (parent != null)
                {
                    Files.createDirectories(parent);
                }

                Files.copy(source.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
                return;
            }
            catch (IOException e)
            {
                if (count == maxTries - 1)
                {
                    throw e;
                }
                delayBeforeRetry(200);
            }
        }

    }

    // Writing a file
    static void writeFile(String fileName, String fileContent) throws InterruptedException
    {

        // Destination file
        int maxTries = 10;
        for (int count = 0; count < maxTries; count++)
        {
            try
            {
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName)))
                {
                    writer.write(fileContent);
                }
                return;
            }
            catch (IOException ex)
            {
                delayBeforeRetry(500);
                System.out.println("> Failed to write "+fileName+" ("+(count+1)+"/"+maxTries+")");
            }
        }

    }
    
    // List of cultivar codes
    public static ArrayList<String> getCultivarCodes(String cropCode)
    {
        int counter = 1;
        ArrayList<String> cultivarList = new ArrayList<>();
        String modelNameVersion = getModelNameVersion(cropCode);
        try
        {
            File file = new File(App.directorySource + cropCode + modelNameVersion + ".CUL");
            try (Scanner sc = new Scanner(file))
            {
                while(sc.hasNextLine())
                {
                    String line = sc.nextLine();
                    if (line.length()>70)
                    {
                        String supposedlyCultivarCode = line.substring(0,6).replaceAll("\\s", "");
                        String supposedlySpace = line.substring(6,7);
                        String supposedlyCultivarName = line.substring(7,24).trim();
                        String flag = line.substring(line.length()-1).trim();

                        if (supposedlyCultivarCode.length()==6
                                && (supposedlySpace).equals(" ")
                                && flag.equals("*"))
                        {
                            String cultivarCodeAndName = supposedlyCultivarCode+" "+supposedlyCultivarName;
                            if (App.numberOfCultivars>0)
                            {
                                if (counter<=App.numberOfCultivars)
                                    cultivarList.add(cultivarCodeAndName);
                            }
                            else
                            {
                                cultivarList.add(cultivarCodeAndName);
                            }
                            counter++;
                        }
                    }
                }
            }
        }
        catch (Exception e) 
        {
            System.err.println("> getCultivarCodes: failed to read CUL file for " + cropCode + " (" + e + ")");
        }
        return cultivarList;
    }
    
    // Look-up table for the model and version number
    public static String getModelNameVersion(String cropCode)
    {
        String version = "048";
        String model = switch (cropCode != null ? cropCode : "") 
        {
            case "BA", "MZ", "SG", "WH" -> "CER";
            case "SB", "FB", "CH" -> "GRO";
            case "TF" -> "APS";
            default -> "";
        };
        return model + version;
    }    

    // Planting date onset criteria per crop
    // Returns: { tminThresholdC, onsetRainfallMm, onsetDays, drySpellMaxDays, windowHalfDays }
    public static int[] getPlantingDateCriteria(String cropCode)
    {
        return switch (cropCode != null ? cropCode : "") {
            case "MZ", "SB"      -> new int[] { 10, 25, 10, 7, 30 };
            case "SG"            -> new int[] { 12, 20, 10, 7, 30 };
            case "WH", "BA"      -> new int[] {  0, 20, 10, 10, 30 };
            case "RI"            -> new int[] { 15, 30, 10, 5, 30 };
            case "FB", "CH"      -> new int[] {  8, 20, 10, 7, 30 };
            case "TF"            -> new int[] {  8, 20, 10, 7, 30 };
            default              -> new int[] {  5, 20, 10, 7, 30 };
        };
    }

    // Convert month to the midday of the month
    public static String getPlantingDate(String plantingMonth)
    {
        int pm = Integer.parseInt(plantingMonth);
        int pd = switch (pm) {
            case 1 -> 15;
            case 2 -> 46;
            case 3 -> 74;
            case 4 -> 105;
            case 5 -> 135;
            case 6 -> 166;
            case 7 -> 196;
            case 8 -> 227;
            case 9 -> 258;
            case 10 -> 288;
            case 11 -> 319;
            case 12 -> 349;
            default -> 0;
        };
        return String.valueOf(pd);
    }

    // Delete temporary output files from the [summary] directory to save storage space
    public static void deleteSummaryFiles() throws InterruptedException
    {
        File folder = new File(App.directoryOutput);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".csv"));
        for (File file : files)
        {
            if (!file.delete())
            {

                // wait a bit then retry on Windows
                if (file.exists())
                {
                    for (int i = 0; i < 6; i++)
                    {
                        delayBeforeRetry(500);
                        System.gc();
                        if (file.delete())
                            break;
                    }
                }
            }
        }
    }
    public static void deleteSummaryFiles(String wthCode) throws InterruptedException
    {
        File folder = new File(App.directoryOutput);
        File[] files = folder.listFiles((dir, name) -> name.endsWith("_Q"+wthCode+".csv"));
        for (File file : files)
        {
            if (!file.delete())
            {

                // wait a bit then retry on Windows
                if (file.exists())
                {
                    for (int i = 0; i < 6; i++)
                    {
                        delayBeforeRetry(500);
                        System.gc();
                        if (file.delete())
                            break;
                    }
                }
            }
        }
    }    

    // Parse US.SOL into a map of soilProfileId -> full DSSAT profile text.
    // Profiles are blank-line-separated blocks; each begins with "*<id> ...".
    static Map<String, String> loadSoilProfiles(String path)
    {
        Map<String, String> profiles = new HashMap<>();
        File file = new File(path);
        if (!file.isFile())
        {
            System.err.println("> loadSoilProfiles: soil file not found: " + path);
            return profiles;
        }
        String nl = System.lineSeparator();
        try (BufferedReader reader = new BufferedReader(new FileReader(path)))
        {
            String line;
            String currentId = null;
            StringBuilder block = new StringBuilder();
            while ((line = reader.readLine()) != null)
            {
                if (line.startsWith("*"))
                {
                    if (currentId != null)
                        profiles.put(currentId, block.toString().stripTrailing());
                    currentId = line.substring(1).trim().split("\\s+")[0];
                    block = new StringBuilder();
                    block.append(line).append(nl);
                }
                else if (currentId != null)
                {
                    block.append(line).append(nl);
                }
            }
            if (currentId != null)
                profiles.put(currentId, block.toString().stripTrailing());
        }
        catch (IOException e)
        {
            System.err.println("> loadSoilProfiles: failed to read " + path + " (" + e + ")");
        }
        return profiles;
    }

    // List of Unit IDs — read from the JSONL table (one cell per line, crops
    // nested) and resolve each cell's soil profile from US.SOL by soilProfileId.
    // Each returned Object[16] is one (cell, crop, cultivar) unit; the positional
    // layout is unchanged from the previous CSV reader so downstream code is intact.
    @SuppressWarnings("unchecked")
    public static Object[] getUnitInfo(String tableName, String directoryInput, int limitForDebugging)
    {
        int counter = 0;
        List<Object[]> unitInfo = Lists.newArrayList();

        // Soil profiles now live in a separate US.SOL, keyed by soilProfileId.
        Map<String, String> soilProfiles = loadSoilProfiles(directoryInput + "US.SOL");

        // SafeConstructor: parse only standard scalar/list/map types (our data),
        // never instantiate arbitrary classes from YAML tags.
        Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
        String jsonlPath = directoryInput + tableName + ".jsonl";

        try (BufferedReader reader = new BufferedReader(new FileReader(jsonlPath)))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (limitForDebugging != 0 && counter >= limitForDebugging) break;
                line = line.strip();
                if (line.isEmpty()) continue;

                Map<String, Object> cell;
                try
                {
                    cell = yaml.load(line);
                }
                catch (RuntimeException ex)
                {
                    System.err.println("> getUnitInfo: skipping unparseable line (" + ex + ")");
                    continue;
                }

                try
                {
                    int unitId = ((Number) cell.get("unitId")).intValue();
                    int cell5m = ((Number) cell.get("cell5m")).intValue();
                    double x = ((Number) cell.get("x")).doubleValue();
                    double y = ((Number) cell.get("y")).doubleValue();
                    String soilProfileId = (String) cell.get("soilProfileId");
                    int soilRootingDepth = ((Number) cell.get("soilRootingDepth")).intValue();

                    String soilProfile = soilProfiles.get(soilProfileId);
                    if (soilProfile == null)
                    {
                        System.err.println("> getUnitInfo: skipping UnitID " + unitId + " (CELL5M " + cell5m
                                + "): soil profile " + soilProfileId + " not found in US.SOL");
                        continue;
                    }

                    List<Map<String, Object>> crops = (List<Map<String, Object>>) cell.get("crops");
                    for (Map<String, Object> crop : crops)
                    {
                        String cropCode = (String) crop.get("code");
                        int plantingDate = ((Number) crop.get("plantingDate")).intValue();
                        double area = ((Number) crop.get("area")).doubleValue();
                        double nFertRateAct = ((Number) crop.get("nFertRateAct")).doubleValue();
                        double nFertRateRec = ((Number) crop.get("nFertRateRec")).doubleValue();
                        String waterSupply = (String) crop.get("waterSupply");
                        double plantingDensity = ((Number) crop.get("plantingDensity")).doubleValue();

                        for (String cultivarCodeAndName : getCultivarCodes(cropCode))
                        {
                            String cultivarCode = cultivarCodeAndName.substring(0, 6);
                            String cultivarName = cultivarCodeAndName.substring(7);

                            // Putting all unit information in one object array
                            Object[] o = new Object[16];
                            o[0]  = unitId;
                            o[1]  = cell5m;
                            o[2]  = x;
                            o[3]  = y;
                            o[4]  = soilProfileId;
                            o[5]  = soilProfile;
                            o[6]  = soilRootingDepth;
                            o[7]  = plantingDate;
                            o[8]  = cropCode;
                            o[9]  = cultivarCode;
                            o[10] = cultivarName;
                            o[11] = nFertRateAct;
                            o[12] = nFertRateRec;
                            o[13] = waterSupply;
                            o[14] = plantingDensity;
                            o[15] = area;
                            unitInfo.add(o);
                            counter++;
                        }
                    }
                }
                catch (RuntimeException ex)
                {
                    System.err.println("> getUnitInfo: failed to parse cell " + cell.get("cell5m") + " (" + ex + ")");
                }
            }
        }
        catch (IOException e)
        {
            System.err.println("> getUnitInfo: failed to read " + jsonlPath + " (" + e + ")");
        }
        return unitInfo.toArray(Object[]::new);
    }

    // CO2
    public static TreeMap<Integer, Integer> getCO2History(String directoryInput)
    {
        TreeMap<Integer, Integer> co2History = new TreeMap<>();
        try
        {
            try (Reader in = new FileReader(directoryInput + "CO2048.csv"))
            {
                var format = CSVFormat.RFC4180.builder().setSkipHeaderRecord(true).setHeader().get();
                Iterable<CSVRecord> records = format.parse(in);
                for (CSVRecord record : records)
                {
                    int y = Integer.parseInt(record.get("YEAR"));
                    int c = (int)Double.parseDouble(record.get("CO2"));
                    co2History.put(y,c);
                }
            }
        }
        catch (IOException e) 
        {
            System.err.println("> getCO2History: failed to read CO2048.csv (" + e + ")");
        }
        return co2History;
    }

    // File names
    static String[] getFileNames(String filePath, String filteringText)
    {
        File dir = new File(filePath);
        FilenameFilter filter = (directory, name) -> (name.toUpperCase().contains(filteringText.toUpperCase()));
        return dir.list(filter);
    }
    static String[] getFileNames(String filePath, String filteringText, int limitForDebugging)
    {
        File dir = new File(filePath);
        FilenameFilter filter = (directory, name) -> (name.toUpperCase().contains(filteringText.toUpperCase()));
        String[] out;
        if (limitForDebugging>0)
            out = Arrays.stream(dir.list(filter)).limit(limitForDebugging).toArray(String[]::new);
        else
            out = Arrays.stream(dir.list(filter)).toArray(String[]::new);
        return out;
    }
    static String[] getFileNames(String filePath)
    {
        File dir = new File(filePath);
        return dir.list();
    }

    // Print object array in string
    static String getString(Object[] items)
    {
        StringBuilder s = new StringBuilder();
        for (Object item: items)
            s.append(item).append(", ");
        s = new StringBuilder(s.substring(0, s.length() - 2));
        return s.toString();
    }

    // OS detection
    public static boolean isWindows()
    {
        return (OS.contains("win"));
    }
    public static boolean isUnix()
    {
        return (OS.contains("nix") || OS.contains("nux") || OS.indexOf("aix") > 0 );
    }
    
    // Numeric checker
    static boolean isNumeric(String strNum)
    {
        if (strNum == null) {
            return false;
        }
        return pattern.matcher(strNum).matches();
    }
}
