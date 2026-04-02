package org.cgiar.tokki;

// Java utilities
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

// ThreadSeasonalRuns class
public class ThreadSeasonalRuns implements Callable<Integer>
{
    int exitCode = 0;
    Object[] o;
    Object[] weatherAndPlantingDate;
    Object[] cultivarOption;
    int daysToFlowering;     // cultivar-level fallback
    int daysToHarvest;       // cultivar-level fallback
    TreeMap<Integer, int[]> yearToDtf;  // year → {dtf, dth}; overrides fallback when present
    String progress;
    int firstPlantingYear;
    int numberOfYears;
    TreeMap<Integer, Integer> co2History;
    DecimalFormat df000 = new DecimalFormat("000");

    ThreadSeasonalRuns(Object[] o, Object[] weatherAndPlantingDate, Object[] cultivarOption,
                       int daysToFlowering, int daysToHarvest, TreeMap<Integer, int[]> yearToDtf,
                       String progress, int firstPlantingYear, int numberOfYears, TreeMap<Integer, Integer> co2History)
    {
        this.o = o;
        this.weatherAndPlantingDate = weatherAndPlantingDate;
        this.cultivarOption = cultivarOption;
        this.daysToFlowering = daysToFlowering;
        this.daysToHarvest = daysToHarvest;
        this.yearToDtf = yearToDtf;
        this.progress = progress;
        this.firstPlantingYear = firstPlantingYear;
        this.numberOfYears = numberOfYears;
        this.co2History = co2History;
    }

    @Override
    public Integer call() throws IOException
    {

        // Modeling unit information
        int unitId = (Integer)o[0];
        int cell5m = (Integer)o[1];
        String soilProfileID = (String)o[2];
        String soilProfile = (String)o[3];
        int soilRootingDepth = (Integer)o[4];
        DecimalFormat dfTT = new DecimalFormat("00");

        // Thread ID?
        int threadID = Integer.parseInt(Thread.currentThread().getName());

        // Copy weather file
        boolean weatherFound = false;
        String weatherFileName = "";
        try
        {
            weatherFileName = (String)weatherAndPlantingDate[0];
            File weatherSource = new File(App.directoryWeather+weatherFileName);
            File weatherDestination = new File(App.directoryThreads+"T"+threadID+App.d+"WEATHERS.WTG");
            Utility.copyFileUsingStream(weatherSource, weatherDestination);
            weatherFound = true;
        }
        catch (InterruptedException exception)
        {
            System.out.println("> Seasonal runs: Weather file NOT copied: "+weatherFileName);
        }

        // Proceed only when the weather file is ready
        if (weatherFound)
        {

            // Write soil profile
            soilProfile = Utility.updateSoilProfileDepth(soilProfile, soilRootingDepth);

            // Write soil file
            String soilFile = App.directoryThreads+"T"+threadID+ App.d+soilProfileID.substring(0,2)+".SOL";
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(soilFile)))
            {
                writer.write(soilProfile);
            }
            catch (IOException e)
            {
                System.err.println("> Seasonal: Failed to write soil file " + soilFile + " (" + e + ")");
            }

            // Cultivar code
            String cropCode = ((String)cultivarOption[1]);
            String cultivarCode = ((String)cultivarOption[2]);

            // Planting date option
            String pdateOption;
            String pdensityOption;
            String runLabel;
            String waterManagement;

            // Recommended N application rate
            int recommendedNitrogenRate = (int)cultivarOption[6];
            int actualNitrogenRate = recommendedNitrogenRate/2;
            int[] nitrogenFertilizerRates = new int[]{ actualNitrogenRate, recommendedNitrogenRate };

            // Organic manure application (x1000)
            int[] manureRates = new int[]{ 0, 1 };

            // Residue harvest rate
            int[] residueHarvestPcts = new int[]{ 100, 0 };

            // Combinations?
            ArrayList<Object> scenarios = new ArrayList<>();
            if (App.scenarioCombinations)
            {

                // All combinations
                int wMax = 2, fMax = 2, mMax = 2, rMax = 2, pMax = 2, dMax = 2;

                // Or not
                if (!App.switchScenarios[0]) wMax = 1;
                if (!App.switchScenarios[1]) fMax = 1;
                if (!App.switchScenarios[2]) mMax = 1;
                if (!App.switchScenarios[3]) rMax = 1;
                if (!App.switchScenarios[4]) pMax = 1;
                if (!App.switchScenarios[5]) dMax = 1;

                // Looping
                for (int w=0; w<wMax; w++)
                    for (int f=0; f<fMax; f++)
                        for (int m=0; m<mMax; m++)
                            for (int r=0; r<rMax; r++)
                                for (int p=0; p<pMax; p++)
                                    for (int d=0; d<dMax; d++)
                                    {

                                        // For CO2 fertilization, if the switch is on, no need to simulate the baseline value.
                                        int[] scn;
                                        if (App.switchScenarios[6])
                                            scn = new int[]{ w, f, m, r, p, d, 1 };
                                        else
                                            scn = new int[]{ w, f, m, r, p, d, 0 };
                                        scenarios.add(scn);

                                    }


            }
            else
            {

                // One scenario for each choice
                /*
                0: Water Management
                1: Fertilizer
                2: Manure
                3: Residue
                4: Planting Window
                5: Planting Density
                6: CO2 Fertilization
                */
                for (int s=0; s<App.switchScenarios.length; s++)
                {
                    int switchWaterManagement = 0;
                    int switchFertilizer = 0;
                    int switchManure  = 0;
                    int switchResidue = 0;
                    int switchPlantingWindow = 0;
                    int switchPlantingDensity = 0;
                    int switchCO2Fertilization = 0;

                    switch (s)
                    {
                        case 0 -> {
                            if (App.switchScenarios[s])
                            {
                                switchWaterManagement = 1;
                                int[] scn = { switchWaterManagement, switchFertilizer, switchManure, switchResidue, switchPlantingWindow, switchPlantingDensity, switchCO2Fertilization };
                                scenarios.add(scn);
                            }
                        }
                        case 1 -> {
                            if (App.switchScenarios[s])
                            {
                                switchFertilizer = 1;
                                int[] scn = { switchWaterManagement, switchFertilizer, switchManure, switchResidue, switchPlantingWindow, switchPlantingDensity, switchCO2Fertilization };
                                scenarios.add(scn);
                            }
                        }
                        case 2 -> {
                            if (App.switchScenarios[s])
                            {
                                switchManure = 1;
                                int[] scn = { switchWaterManagement, switchFertilizer, switchManure, switchResidue, switchPlantingWindow, switchPlantingDensity, switchCO2Fertilization };
                                scenarios.add(scn);
                            }
                        }
                        case 3 -> {
                            if (App.switchScenarios[s])
                            {
                                switchResidue = 1;
                                int[] scn = { switchWaterManagement, switchFertilizer, switchManure, switchResidue, switchPlantingWindow, switchPlantingDensity, switchCO2Fertilization };
                                scenarios.add(scn);
                            }
                        }
                        case 4 -> {
                            if (App.switchScenarios[s])
                            {
                                switchPlantingWindow = 1;
                                int[] scn = { switchWaterManagement, switchFertilizer, switchManure, switchResidue, switchPlantingWindow, switchPlantingDensity, switchCO2Fertilization };
                                scenarios.add(scn);
                            }
                        }
                        case 5 -> {
                            if (App.switchScenarios[s])
                            {
                                switchPlantingDensity = 1;
                                int[] scn = { switchWaterManagement, switchFertilizer, switchManure, switchResidue, switchPlantingWindow, switchPlantingDensity, switchCO2Fertilization };
                                scenarios.add(scn);
                            }
                        }
                        case 6 -> {
                            if (App.switchScenarios[s])
                            {
                                switchCO2Fertilization = 1;
                                int[] scn = { switchWaterManagement, switchFertilizer, switchManure, switchResidue, switchPlantingWindow, switchPlantingDensity, switchCO2Fertilization };
                                scenarios.add(scn);
                            }
                        }
                        default -> { }
                    }

                }

            }

            // Scenario
            int ns = scenarios.size();
            for (int s=0; s<ns; s++)
            {

                // What to simulate this time
                int[] scn = (int[])scenarios.get(s);
                int switchWaterManagement = scn[0];
                int switchFertilizer = scn[1];
                int switchManure = scn[2];
                int switchResidue = scn[3];
                int switchPlantingWindow = scn[4];
                int switchPlantingDensity = scn[5];
                int switchCO2Fertilization = scn[6];

                // N rates to use
                ArrayList<Integer> nRates = new ArrayList<>();
                if (App.useRecommendedNitrogenFertilizerRate)
                    nRates.add(nitrogenFertilizerRates[switchFertilizer]);
                else
                    for (Object n: App.nitrogenFertilizerRates)
                        nRates.add((Integer)n);

                // CO2 values to use
                ArrayList<Integer> CO2s = new ArrayList<>();
                if (switchCO2Fertilization==1)
                    for (Object c: App.atmosphericCO2Values)
                        CO2s.add((Integer)c);
                else
                {
                    int y = App.firstPlantingYear;
                    CO2s.add(co2History.get(y));
                }

                // Preparing the combination of multiple N and CO2 values
                ArrayList<Object> inputCombinations = new ArrayList<>();
                for (Object n: nRates)
                    for (Object c: CO2s)
                        inputCombinations.add(new Object[]{ n, c });

                // Looping through the values
                for (Object ic: inputCombinations)
                {
                    Object[] i = (Object[])ic;

                    // Settings
                    int nRate = (Integer)i[0];
                    int co2 = (Integer)i[1];
                    int manureRate = manureRates[switchManure];
                    int residueHarvestPct = residueHarvestPcts[switchResidue];

                    // Treatment label
                    String label = "W"+scn[0]+"F"+df000.format(nRate)+"C"+df000.format(co2);

                    // Water management and planting window
                    if (switchPlantingWindow==0)
                    {

                        // Median yields
                        pdateOption = "PM";
                        if (switchWaterManagement==0)
                        {
                            // 0: pdRainfedMax, 1: pdRainfedMedian, 2: pdIrrigatedMax, 3: pdIrrigatedMedian
                            waterManagement = "R";
                            //plantingDate = plantingDatesRainfed[1];
                        }
                        else
                        {
                            // 0: pdRainfedMax, 1: pdRainfedMedian, 2: pdIrrigatedMax, 3: pdIrrigatedMedian
                            waterManagement = "I";
                            //plantingDate = plantingDatesIrrigated[1];
                        }
                    }
                    else
                    {

                        // Best yields
                        pdateOption = "PB";
                        if (switchWaterManagement==0)
                        {
                            // 0: pdRainfedMax, 1: pdRainfedMedian, 2: pdIrrigatedMax, 3: pdIrrigatedMedian
                            waterManagement = "R";
                            //plantingDate = plantingDatesRainfed[0];
                        }
                        else
                        {
                            // 0: pdRainfedMax, 1: pdRainfedMedian, 2: pdIrrigatedMax, 3: pdIrrigatedMedian
                            waterManagement = "I";
                            //plantingDate = plantingDatesIrrigated[0];
                        }
                    }

                    // Planting density option
                    if (switchPlantingDensity==0)
                    {
                        pdensityOption = "DL";
                    }
                    else
                    {
                        pdensityOption = "DH";
                    }

                    // Status label (year added below)
                    runLabel = "W" + switchWaterManagement + "-F" + df000.format(nRate) + "-M" + switchManure + "-R" + switchResidue + "-" + pdateOption + "-" + pdensityOption + "-" + cropCode + cultivarCode + "-C" + df000.format(co2);

                    // Year loop — run one DSSAT season per simulation year
                    // (weatherFileName already set at the top of call())
                    @SuppressWarnings("unchecked")
                    TreeMap<Integer, Integer> yearToPlantingDate = (TreeMap<Integer, Integer>) weatherAndPlantingDate[1];
                    for (Map.Entry<Integer, Integer> yearEntry : yearToPlantingDate.entrySet())
                    {
                        int simYear = yearEntry.getKey();
                        int pdate   = yearEntry.getValue();

                        // Year-specific DTF/DTH — override cultivar-level fallback when available
                        int dtfForYear = daysToFlowering;
                        int dthForYear = daysToHarvest;
                        if (yearToDtf != null && yearToDtf.containsKey(simYear))
                        {
                            dtfForYear = yearToDtf.get(simYear)[0];
                            dthForYear = yearToDtf.get(simYear)[1];
                        }

                        String runLabelYear = runLabel + "-Y" + simYear;
                        try
                        {
                            SnxWriterSeasonalRuns.runningTreatmentPackages(o, waterManagement, nRate, manureRate, cultivarOption, dtfForYear, dthForYear, pdensityOption, residueHarvestPct, co2, weatherFileName, pdate, label, simYear);
                            System.out.println("> T" + dfTT.format(threadID) + ", " + progress + ", S" + (s+1) + "/" + ns + ", " + runLabelYear);
                            exitCode = ExeRunner.dscsm048_seasonal("N");
                            if (exitCode == 0)
                            {
                                File outputSource = new File(App.directoryThreads + "T" + threadID + App.d + "summary.csv");
                                File outputDestination = new File(App.directoryOutput + "U" + unitId + "_C" + cell5m + "_Y" + simYear + "_S" + s + "_" + runLabelYear + ".csv");
                                outputDestination.setReadable(true, false);
                                outputDestination.setExecutable(true, false);
                                outputDestination.setWritable(true, false);
                                Utility.copyFileUsingStream(outputSource, outputDestination);
                            }
                        }
                        catch (IOException | InterruptedException ex)
                        {
                            System.err.println("> Seasonal runs: Error at T" + threadID + " for S" + s + "_" + runLabelYear + " (" + ex + ")");
                        }
                    } // year loop

                } // for (Object i: inputCombinations)

            }

        }

        // Return
        return exitCode;

    }
    
}
