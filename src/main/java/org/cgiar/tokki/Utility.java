package org.cgiar.tokki;

// Java utilities
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
import java.util.List;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.regex.Pattern;

// Apache utilities
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

// Google utilities
import com.google.common.collect.Lists;

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

    // List of Unit IDs
    public static Object[] getUnitInfo(String tableName, String directoryInput, int limitForDebugging)
    {
        int counter = 0;
        List<Object[]> unitInfo = Lists.newArrayList();
        try
        {
            try (Reader in = new FileReader(directoryInput + tableName + ".csv"))
            {
                var format = CSVFormat.RFC4180.builder().setSkipHeaderRecord(true).setHeader().get();
                Iterable<CSVRecord> records = format.parse(in);
                for (CSVRecord record : records)
                {
                    if (limitForDebugging==0 || counter<limitForDebugging)
                    {
                        String[] crops = record.get("Crops").split(",");
                        String[] pdates = record.get("PlantingDates").split(",");
                        String[] areas = record.get("Areas").split(",");
                        String[] nFertRatesAct = record.get("NFertRateAct").split(",");
                        String[] nFertRatesRec = record.get("NFertRateRec").split(",");
                        String[] waterSupplies = record.get("WaterSupply").split(",");
                        String[] plantingDensities = record.get("PlantingDensity").split(",");

                        for (int c=0; c<crops.length; c++)
                        {
                            String crop = crops[c];
                            String area = areas[c];
                            String nFertRateAct = nFertRatesAct[c];
                            String nFertRateRec = nFertRatesRec[c];
                            String waterSupply = waterSupplies[c];
                            String plantingDensity = plantingDensities[c];
                            ArrayList<String> cultivarList = getCultivarCodes(crop);
                            String[] cultivarCodeAndNames = cultivarList.toArray(String[]::new);
                            try
                            {

                                for (String cultivarCodeAndName: cultivarCodeAndNames)
                                {
                                    String cultivarCode = cultivarCodeAndName.substring(0,6);
                                    String cultivarName = cultivarCodeAndName.substring(7);
                                    String pdate = pdates[c];

                                    // Putting all unit information in one object array
                                    Object[] o = new Object[16];
                                    o[0]  = Integer.valueOf(record.get("UnitID"));
                                    o[1]  = Integer.valueOf(record.get("CELL5M"));
                                    o[2]  = Double.valueOf(record.get("X"));
                                    o[3]  = Double.valueOf(record.get("Y"));
                                    o[4]  = record.get("SoilProfileID");
                                    o[5]  = record.get("SoilProfile");
                                    o[6]  = Integer.valueOf(record.get("SoilRootingDepth"));
                                    o[7]  = Integer.valueOf(pdate);
                                    o[8]  = crop;
                                    o[9]  = cultivarCode;
                                    o[10] = cultivarName;
                                    o[11] = Double.valueOf(nFertRateAct);
                                    o[12] = Double.valueOf(nFertRateRec);
                                    o[13] = waterSupply;
                                    o[14] = Double.valueOf(plantingDensity);
                                    o[15] = area;
                                    unitInfo.add(o);
                                    counter++;
                                }

                            }
                            catch (NumberFormatException | StringIndexOutOfBoundsException | ArrayIndexOutOfBoundsException ex)
                            {
                                System.err.println("> getUnitInfo: failed to parse unit/cultivar row (" + ex + ")");
                            }
                        }
                    }
                }
            }
        }
        catch (IOException e)
        {
            System.err.println("> getUnitInfo: failed to read " + tableName + " (" + e + ")");
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
