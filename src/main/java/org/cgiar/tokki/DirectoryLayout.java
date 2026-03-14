package org.cgiar.tokki;

/**
 * Centralizes all directory paths used by the application.
 * Paths remain as Strings to minimize invasive changes in the current codebase.
 */
public record DirectoryLayout(
        String working,
        String weather,
        String source,
        String input,
        String threads,
        String result,
        String outputSummary,
        String tempPlanting,
        String tempFlowering,
        String tempPlantingDates,
        String tempErrors
) 
{
}