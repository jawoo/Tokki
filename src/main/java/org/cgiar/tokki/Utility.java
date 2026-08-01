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

    // --- Latitude-aware maturity selection --------------------------------------
    // Corn (MZ) and soybean (SB) are simulated with a single cultivar chosen by the
    // cell's latitude, rather than one fixed cultivar everywhere. Soybean maturity
    // group and corn relative maturity both shorten toward the poles: a single
    // southern cultivar run in the north cannot reach maturity (large low bias and
    // failed runs), while a single northern cultivar run in the south wastes the
    // season. The generic maturity cultivars shipped in the DSSAT CUL files provide
    // the zone set. Thresholds live in maturityTarget() for easy recalibration.

    /** Crops whose cultivar is picked by latitude instead of cross-producted. */
    private static boolean hasLatitudeZone(String cropCode)
    {
        return cropCode.equals("SB") || cropCode.equals("MZ");
    }

    /** Cultivars to run at one cell: a single latitude-matched entry for zone crops,
     *  otherwise every flagged cultivar (unchanged behaviour for other crops). */
    public static List<String> cultivarsForCell(String cropCode, double latitude)
    {
        if (!hasLatitudeZone(cropCode))
            return getCultivarCodes(cropCode);
        String chosen = selectMaturityCultivar(cropCode, latitude);
        return chosen == null ? new ArrayList<>() : List.of(chosen);
    }

    // Generic maturity cultivars per crop, parsed from the CUL once and cached.
    // Key = maturity index (soybean: maturity group, 000=-2, 00=-1, 0..10;
    // maize: season rank V.SHORT=0, SHORT=1, MEDIUM=2, LONG=3). Value = "VAR# NAME".
    private static final Map<String, TreeMap<Double, String>> maturityCache = new HashMap<>();

    static TreeMap<Double, String> getMaturityCultivars(String cropCode)
    {
        TreeMap<Double, String> cached = maturityCache.get(cropCode);
        if (cached != null) return cached;

        TreeMap<Double, String> map = new TreeMap<>();
        String modelNameVersion = getModelNameVersion(cropCode);
        File file = new File(App.directorySource + cropCode + modelNameVersion + ".CUL");
        try (Scanner sc = new Scanner(file))
        {
            while (sc.hasNextLine())
            {
                String line = sc.nextLine();
                if (line.length() <= 70) continue;               // same guard as getCultivarCodes
                String code = line.substring(0, 6).trim();
                String name = line.substring(7, 24).trim();
                Double index = maturityIndex(cropCode, name);
                if (index != null)
                    map.putIfAbsent(index, code + " " + name);   // first (canonical) cultivar per index
            }
        }
        catch (Exception e)
        {
            System.err.println("> getMaturityCultivars: failed to read CUL for " + cropCode + " (" + e + ")");
        }
        maturityCache.put(cropCode, map);
        return map;
    }

    // Maturity index for a generic cultivar name, or null if the line is not one of
    // the generic maturity cultivars we assign by latitude.
    private static Double maturityIndex(String cropCode, String name)
    {
        if (cropCode.equals("SB"))
        {
            // "M GROUP 000" | "M GROUP 00" | "M GROUP 0" | "M GROUP 1" .. "M GROUP 10"
            if (!name.startsWith("M GROUP")) return null;
            String tok = name.substring(7).trim();
            if (tok.equals("000")) return -2.0;
            if (tok.equals("00"))  return -1.0;
            try { return (double) Integer.parseInt(tok); }
            catch (NumberFormatException ex) { return null; }    // named lines e.g. Savoy, Vinton
        }
        if (cropCode.equals("MZ"))
        {
            switch (name)
            {
                case "V.SHORT SEASON": return 0.0;
                case "SHORT SEASON":   return 1.0;
                case "MEDIUM SEASON":  return 2.0;
                case "LONG SEASON":    return 3.0;
                default:               return null;
            }
        }
        return null;
    }

    /** The generic cultivar nearest the latitude-implied maturity target, or null. */
    static String selectMaturityCultivar(String cropCode, double latitude)
    {
        TreeMap<Double, String> map = getMaturityCultivars(cropCode);
        if (map.isEmpty())
        {
            System.err.println("> selectMaturityCultivar: no generic maturity cultivars for " + cropCode
                    + "; check " + cropCode + getModelNameVersion(cropCode) + ".CUL");
            return null;
        }
        double target = maturityTarget(cropCode, Math.abs(latitude));
        double bestKey = map.firstKey();
        double bestDist = Math.abs(bestKey - target);
        for (double key : map.keySet())
        {
            double d = Math.abs(key - target);
            if (d < bestDist) { bestDist = d; bestKey = key; }
        }
        return map.get(bestKey);
    }

    // Latitude -> maturity target; both crops shorten toward the poles.
    private static double maturityTarget(String cropCode, double absLat)
    {
        if (cropCode.equals("SB"))
        {
            // Soybean MG ~0 near 48 deg N to ~7 near 30 deg N (linear), clamped to a
            // sensible US range: MG = (48 - lat) * 7/18.
            double mg = (48.0 - absLat) * 7.0 / 18.0;
            return Math.max(0.0, Math.min(8.0, mg));
        }
        // Corn relative maturity: full season through most of the belt, shortening
        // only at the northern fringe. Ranks SHORT=1, MEDIUM=2, LONG=3 (V.SHORT=0
        // is reserved and never targeted).
        if (absLat >= 47.0) return 1.0;   // northern fringe    -> SHORT
        if (absLat >= 44.0) return 2.0;   // upper Midwest      -> MEDIUM
        return 3.0;                        // core/southern belt -> LONG
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

    // Load a JSON Schema file (valid YAML) into a Map tree; null if unavailable.
    @SuppressWarnings("unchecked")
    static Map<String, Object> loadSchema(String path)
    {
        if (!new File(path).isFile())
            return null;
        try (BufferedReader reader = new BufferedReader(new FileReader(path)))
        {
            return new Yaml(new SafeConstructor(new LoaderOptions())).load(reader);
        }
        catch (IOException | RuntimeException e)
        {
            System.err.println("> loadSchema: failed to read " + path + " (" + e + ")");
            return null;
        }
    }

    // List of Unit IDs — read from the JSONL table (one cell per line, crops
    // nested) and resolve each cell's soil profile from US.SOL by soilProfileId.
    // Each returned Object[16] is one (cell, crop, cultivar) unit; the positional
    // layout is unchanged from the previous CSV reader so downstream code is intact.
    @SuppressWarnings("unchecked")
    public static Object[] getUnitInfo(String tableName, String directoryInput, int limitForDebugging)
    {
        int counter = 0;
        int skippedInvalid = 0;
        List<Object[]> unitInfo = Lists.newArrayList();

        // Soil profiles now live in a separate US.SOL, keyed by soilProfileId.
        Map<String, String> soilProfiles = loadSoilProfiles(directoryInput + "US.SOL");

        // Validate each record against the JSON Schema so the running model
        // rejects exactly what the converter would (single source of truth).
        SchemaValidator validator = null;
        Map<String, Object> schema = loadSchema(directoryInput + "unit-information.schema.json");
        if (schema != null)
            validator = new SchemaValidator(schema);
        else
            System.err.println("> getUnitInfo: schema not found; skipping schema validation");

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

                if (validator != null)
                {
                    List<String> schemaErrors = validator.validate(cell);
                    if (!schemaErrors.isEmpty())
                    {
                        skippedInvalid++;
                        System.err.println("> getUnitInfo: schema validation failed for cell "
                                + cell.get("cell5m") + ": " + schemaErrors.get(0)
                                + (schemaErrors.size() > 1 ? " (+" + (schemaErrors.size() - 1) + " more)" : ""));
                        continue;
                    }
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

                        // Corn/soybean get a single latitude-matched maturity cultivar;
                        // other crops keep the full flagged-cultivar cross-product.
                        for (String cultivarCodeAndName : cultivarsForCell(cropCode, y))
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
        if (skippedInvalid > 0)
            System.err.println("> getUnitInfo: skipped " + skippedInvalid + " cell(s) failing schema validation.");
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
