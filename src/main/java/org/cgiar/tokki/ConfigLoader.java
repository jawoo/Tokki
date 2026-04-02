package org.cgiar.tokki;

// Java utilities
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

// SnakeYAML utilities
import org.yaml.snakeyaml.Yaml;

// ConfigLoader class
public final class ConfigLoader 
{
    private ConfigLoader() {}

    @SuppressWarnings("unchecked")
    public static TokkiConfig load(String configPath, String separator) throws IOException {
        Yaml yaml = new Yaml();

        Map<String, Object> config;
        try (InputStream inputStream = new FileInputStream(configPath)) {
            config = yaml.load(inputStream);
        }

        String tableNameUnitInformation = (String) config.get("tableNameUnitInformation");
        String countryCode = (String) config.get("countryCode");
        int numberOfThreads = (int) config.get("numberOfThreads");
        int limitForDebugging = (int) config.get("limitForDebugging");
        boolean scenarioCombinations = (int) config.get("scenarioCombinations") > 0;

        boolean useRecommendedNitrogenFertilizerRate =
                (int) config.get("useRecommendedNitrogenFertilizerRate") > 0;

        List<Integer> nitrogenFertilizerRatesList = (List<Integer>) config.get("nitrogenFertilizerRates");
        Object[] nitrogenFertilizerRates = nitrogenFertilizerRatesList.toArray();

        List<Integer> atmosphericCO2List = (List<Integer>) config.get("atmosphericCO2");
        Object[] atmosphericCO2Values = atmosphericCO2List.toArray();

        Map<String, String> directories = (Map<String, String>) config.get("directory");
        String working = "." + separator + directories.get("working") + separator;
        DirectoryLayout layout = new DirectoryLayout(
                working,
                working + "weather" + separator + directories.get("weather") + separator,
                working + directories.get("source") + separator,
                working + directories.get("input") + separator,
                working + directories.get("threads") + separator,
                working + directories.get("result") + separator,
                working + directories.get("temp") + separator + directories.get("summary") + separator,
                working + directories.get("temp") + separator + directories.get("flowering") + separator,
                working + directories.get("temp") + separator + directories.get("plantingDates") + separator,
                working + directories.get("temp") + separator + directories.get("errors") + separator
        );

        Map<String, Integer> scenarioSwitches = (Map<String, Integer>) config.get("scenarioSwitch");
        boolean[] switchScenarios = new boolean[7];
        switchScenarios[0] = scenarioSwitches.get("waterManagement") > 0;
        switchScenarios[1] = scenarioSwitches.get("fertilizer") > 0;
        switchScenarios[2] = scenarioSwitches.get("manure") > 0;
        switchScenarios[3] = scenarioSwitches.get("residue") > 0;
        switchScenarios[4] = scenarioSwitches.get("plantingWindow") > 0;
        switchScenarios[5] = scenarioSwitches.get("plantingDensity") > 0;
        switchScenarios[6] = scenarioSwitches.get("CO2fertilization") > 0;

        Map<String, Integer> plantingDateOptions = (Map<String, Integer>) config.get("plantingDateOptions");
        boolean useFixedPlantingDate = plantingDateOptions.get("useFixedPlantingDate") > 0;
        int fixedPlantingDate = plantingDateOptions.get("fixedPlantingDate");
        int firstPlantingYear = (int)config.get("firstPlantingYear");
        int numberOfYears = (int)config.get("numberOfYears");
        int latBandSize = config.containsKey("latBandSize") ? (int) config.get("latBandSize") : 10;

        return new TokkiConfig(
                tableNameUnitInformation,
                countryCode,
                numberOfThreads,
                limitForDebugging,
                scenarioCombinations,
                useRecommendedNitrogenFertilizerRate,
                nitrogenFertilizerRates,
                atmosphericCO2Values,
                layout,
                switchScenarios,
                useFixedPlantingDate,
                fixedPlantingDate,
                firstPlantingYear,
                numberOfYears,
                latBandSize
        );
    }
}

