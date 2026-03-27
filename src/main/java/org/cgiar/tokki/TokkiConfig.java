package org.cgiar.tokki;

public record TokkiConfig(
        String tableNameUnitInformation,
        String countryCode,
        int numberOfThreads,
        int limitForDebugging,
        boolean scenarioCombinations,
        boolean useRecommendedNitrogenFertilizerRate,
        Object[] nitrogenFertilizerRates,
        Object[] atmosphericCO2Values,
        DirectoryLayout directories,
        String dataPlantingDates,
        boolean[] switchScenarios,
        boolean useFixedPlantingDate,
        int fixedPlantingDate,
        int firstPlantingYear,
        int numberOfYears
) 
{
}