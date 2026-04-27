package org.cgiar.tokki;

// Java utilities
import java.text.DecimalFormat;

// SnxWriterFloweringRuns class
public class SnxWriterFloweringRuns 
{
    static DecimalFormat dfTT  = new DecimalFormat("00");
    static DecimalFormat dfDDD  = new DecimalFormat("000");

    public static void runningTreatmentPackages(
            int threadID,
            Object[] o,
            int pd,
            Object[] cultivarOption,
            int co2,
            int simYear
            ) throws NumberFormatException, InterruptedException {

        // Unit information
        String soilProfileID = (String)o[4];
        int soilRootingDepth = (Integer)o[6];

        // Cultivar code
        String cropCode = (String)cultivarOption[1];
        String cultivarCode = (String)cultivarOption[2];

        // YY — two-digit year for this simulation year
        String yy = String.valueOf(simYear).substring(2);

        // Boolean switches
        boolean isRice;
        isRice = cropCode.equals("RI");

        // Plantng density (low)
        String plantingDensity = dfDDD.format((int)cultivarOption[4]);

        // Treatments
        String snxSectionTreatments = """

*TREATMENTS                        -------------FACTOR LEVELS------------
@N R O C TNAME.................... CU FL SA IC MP MI MF MR MC MT ME MH SM
01 1 0 0 %s-P5-FXX-WX           1  1  0  1 01  0  0  0  0  0  1  0  1
""".formatted(cultivarCode);

        // Fields
        String snxSectionFields = """

*FIELDS
@L ID_FIELD WSTA....  FLSA  FLOB  FLDT  FLDD  FLDS  FLST SLTX  SLDP  ID_SOIL    FLNAME
 1 TOUCAN01 WEATHERS   -99     0 IB000     0     0 00000 -99    180  %s -99
@L ...........XCRD ...........YCRD .....ELEV .............AREA .SLEN .FLWR .SLAS FLHST FHDUR
 1               0               0         0                 0     0     0     0   -99   -99
""".formatted(soilProfileID);

        // Initial Conditions
        String icdat = yy+"001";
        String icbl = dfDDD.format(soilRootingDepth);
        String snxSectionInitialConditions = """

*INITIAL CONDITIONS
@C   PCR ICDAT  ICRT  ICND  ICRN  ICRE  ICWD ICRES ICREN ICREP ICRIP ICRID ICNAME
 1    %s %s   100     0     1     1   180  1000    .8     0   100    15 -99
@C  ICBL  SH2O  SNH4  SNO3
 1   %s  .500    .1    .1
""".formatted(cropCode, icdat, icbl);

        // Planting Details
        StringBuilder snxSectionPlantingDetails = new StringBuilder("""

*PLANTING DETAILS
@P PDATE EDATE  PPOP  PPOE  PLME  PLDS  PLRS  PLRD  PLDP  PLWT  PAGE  PENV  PLPH  SPRL                        PLNAME
""");
        if (isRice)
            snxSectionPlantingDetails.append(dfTT.format(1)).append(" ").append(yy).append(dfDDD.format(pd)).append("   -99   ").append(plantingDensity).append("   ").append(plantingDensity).append("     T     H    20     0     2     0    23    25     3     0                        -99\n");
        else
            snxSectionPlantingDetails.append(dfTT.format(1)).append(" ").append(yy).append(dfDDD.format(pd)).append("   -99   ").append(plantingDensity).append("   ").append(plantingDensity).append("     S     R    61     0     7   -99   -99   -99   -99     0                        -99\n");

        // Environment modifications
        String snxSectionEnvironmentModification = """

*ENVIRONMENT MODIFICATIONS
@E ODATE EDAY  ERAD  EMAX  EMIN  ERAIN ECO2  EDEW  EWIND ENVNAME  
 1 %s A 0.0 A 0.0 A   0 A   0 A 0.0 R %s A   0 A   0 
""".formatted(icdat, co2);

        // Simulation controls
        String snxSectionSimulationControls = """

*SIMULATION CONTROLS
@N GENERAL     NYERS NREPS START SDATE RSEED SNAME.................... SMODEL
 1 GE              1     1     S %s  4573
@N OPTIONS     WATER NITRO SYMBI PHOSP POTAS DISES  CHEM  TILL   CO2
 1 OP              N     N     N     N     N     N     N     N     D
@N METHODS     WTHER INCON LIGHT EVAPO INFIL PHOTO HYDRO NSWIT MESOM MESEV MESOL
 1 ME              G     M     E     R     S     C     R     1     G     S     2
@N MANAGEMENT  PLANT IRRIG FERTI RESID HARVS
 1 MA              R     D     D     N     M
@N OUTPUTS     FNAME OVVEW SUMRY FROPT GROUT CAOUT WAOUT NIOUT MIOUT DIOUT VBOSE CHOUT OPOUT FMOPT
 1 OU              N     Y     Y     3     N     N     N     N     N     N     0     N     N     C

@  AUTOMATIC MANAGEMENT
@N PLANTING    PFRST PLAST PH2OL PH2OU PH2OD PSTMX PSTMN
 1 PL          %s %s    40   100    30    40    10
@N IRRIGATION  IMDEP ITHRL ITHRU IROFF IMETH IRAMT IREFF
 1 IR             30    70   100 IB001 IB001    20   .75
@N NITROGEN    NMDEP NMTHR NAMNT NCODE NAOFF
 1 NI             30    50    25 IB001 IB001
@N RESIDUES    RIPCN RTIME RIDEP
 1 RE            100     1    20
@N HARVEST     HFRST HLAST HPCNP HPCNR
 1 HA              0 79065   100     0
""".formatted(icdat, icdat, icdat);

        // SNX
        String snx = """
*EXP.DETAILS: TOUCAN0%sSN FLOWERING DATES

*GENERAL
%s
*CULTIVARS
@C CR INGENO CNAME
 1 %s %s %s
%s%s%s%s%s
""".formatted(
                threadID,
                snxSectionTreatments,
                cultivarOption[1], cultivarOption[2], cultivarOption[3],
                snxSectionFields,
                snxSectionInitialConditions,
                snxSectionPlantingDetails,
                snxSectionEnvironmentModification,
                snxSectionSimulationControls
        );

        // Write
        String snxFile = App.directoryThreads+"T"+threadID+ App.d+"TOUCAN"+dfTT.format(threadID)+".SNX";
        Utility.writeFile(snxFile, snx);

        // Batch file
        StringBuilder batch = new StringBuilder("""
$BATCH(SEASONAL)

@FILEX                                                                                        TRTNO     RP     SQ     OP     CO
""");
        batch.append("TOUCAN").append(dfTT.format(threadID)).append(".SNX                                                                                     ").append(dfTT.format(1)).append("      1      0      0      0\n");

        // Write
        String batchFile = App.directoryThreads+"T"+threadID+App.d+"DSSBatch.v48";
        Utility.writeFile(batchFile, batch.toString());

    }    

}
