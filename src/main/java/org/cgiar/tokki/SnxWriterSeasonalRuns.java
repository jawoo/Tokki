package org.cgiar.tokki;

// Java utilities
import java.text.DecimalFormat;
import java.util.Map;
import java.util.TreeMap;

// SnxWriterSeasonalRuns class
public class SnxWriterSeasonalRuns 
{
    static DecimalFormat dfTT    = new DecimalFormat("00");
    static DecimalFormat dfDDD   = new DecimalFormat("000");
    static DecimalFormat dfXYCRD = new DecimalFormat("+000.00;-000.00");

    public static void runningTreatmentPackages(
            Object[] o,
            String waterManagement,
            int nRate,
            int manureRate,
            Object[] cultivarOption,
            int daysToFlowering,
            int daysToHarvest,
            String pdensityOption,
            int residueHarvestPct,
            int co2,
            Object[] weatherAndPlantingDate,
            String label,
            int firstPlantingYear,
            int numberOfYears
            ) throws InterruptedException {

        // Thread ID?
        int threadID = Integer.parseInt(Thread.currentThread().getName());

        // YY
        String yy = String.valueOf(firstPlantingYear).substring(2);

        // Unit information
        String soilProfileID = (String)o[2];
        int soilRootingDepth = (Integer)o[4];
        double x = (double)o[10];
        double y = (double)o[11];

        // Cultivar code
        String cropCode = ((String)cultivarOption[1]);

        // Boolean switches
        boolean isRice = cropCode.equals("RI");
        boolean isWheat = cropCode.equals("WH");
        boolean isIrrigated = waterManagement.equals("I");

        // Treatments
        String snxSectionTreatments = """

*TREATMENTS                        -------------FACTOR LEVELS------------
@N R O C TNAME.................... CU FL SA IC MP MI MF MR MC MT ME MH SM
""";
        String mi = "0", mf = "0", mr = "0", mh = "0";
        if (isIrrigated || isRice) mi = "2";
        if (nRate>0) mf = "1";
        if (manureRate>0) mr = "1";
        if (residueHarvestPct<100 || isWheat) mh = "1";

        // Fields
        String idField = cultivarOption[1]+(String)cultivarOption[2];
        String snxSectionFieldLevel1 = "\n*FIELDS\n@L ID_FIELD WSTA....  FLSA  FLOB  FLDT  FLDD  FLDS  FLST SLTX  SLDP  ID_SOIL    FLNAME\n";
        String snxSectionFieldLevel2 = "@L ...........XCRD ...........YCRD .....ELEV .............AREA .SLEN .FLWR .SLAS FLHST FHDUR\n";

        // Fertilizer
        int splitFertilizerDate = daysToFlowering; //30;
        int splitFertilizerRate = nRate/2;
        String snxSectionFertilizer = """

*FERTILIZERS (INORGANIC)
@F FDATE  FMCD  FACD  FDEP  FAMN  FAMP  FAMK  FAMC  FAMO  FOCD FERNAME
 1     1 FE001 AP001    10   %s     0     0     0     0   -99 -99
 1   %s FE001 AP001    10   %s     0     0     0     0   -99 -99
""".formatted(dfDDD.format(splitFertilizerRate), dfDDD.format(splitFertilizerDate), dfDDD.format(splitFertilizerRate));

        // Filling the treatment and field sections
        int tn = 1;

        // Planting
        StringBuilder snxSectionPlantingDetails = new StringBuilder("""

*PLANTING DETAILS
@P PDATE EDATE  PPOP  PPOE  PLME  PLDS  PLRS  PLRD  PLDP  PLWT  PAGE  PENV  PLPH  SPRL                        PLNAME
""");

        // Irrigation
        StringBuilder snxSectionIrrigation = new StringBuilder("\n*IRRIGATION AND WATER MANAGEMENT\n");
        boolean irrigationSectionWritten = false;

        // Batch
        StringBuilder batch = new StringBuilder("""
$BATCH(SEQUENCE)

@FILEX                                                                                        TRTNO     RP     SQ     OP     CO
""");

        // Retrieval
        int pdate = (Integer)weatherAndPlantingDate[1];

        snxSectionTreatments +=
                dfTT.format(tn)+" 1 0 0 "+label+"                 1 "+dfTT.format(tn)+"  0  1 "+dfTT.format(tn)+"  "+mi+"  "+mf+"  "+mr+"  0  0  1  "+mh+"  1\n";

        snxSectionFieldLevel1 +=
                dfTT.format(tn)+" "+idField+" "+"WEATHERS"+"   -99     0 IB000     0     0 00000 -99    180  "+soilProfileID+" -99\n";

        snxSectionFieldLevel2 +=
                dfTT.format(tn)+"         "+dfXYCRD.format(x)+"         "+dfXYCRD.format(y)+"         0                 0     0     0     0 FH102    30\n";

        // Planting Details
        String pdt = dfDDD.format(pdate);

        // Planting density
        String plantingDensity;

        // Low vs high density
        if (String.valueOf(pdensityOption).equals("DL"))
            plantingDensity = dfDDD.format((int)cultivarOption[4]);
        else
            plantingDensity = dfDDD.format((int)cultivarOption[5]);

        // Rice vs non-rice
        if (isRice)
            snxSectionPlantingDetails.append(dfTT.format(tn)).append(" ").append(yy).append(pdt).append("   -99   ").append(plantingDensity).append("   ").append(plantingDensity).append("     T     H    20     0     2     0    23    25     3     0                        -99\n");
        else
            snxSectionPlantingDetails.append(dfTT.format(tn)).append(" ").append(yy).append(pdt).append("   -99   ").append(plantingDensity).append("   ").append(plantingDensity).append("     S     R    61     0     7   -99   -99   -99   -99     0                        -99\n");

        // Irrigation
        if (!irrigationSectionWritten)
        {
            if (isRice)
            {
                if (daysToFlowering>10)
                {
                    snxSectionIrrigation.append("""
@I  EFIR  IDEP  ITHR  IEPT  IOFF  IAME  IAMT IRNAME
 1     1   -99   -99   -99   -99   -99   -99 -99
@I IDATE  IROP IRVAL
 1 %s%s IR008     2
 1 %s%s IR010     0
 1 %s%s IR009   150
 1 %s%s IR003    30
@I  EFIR  IDEP  ITHR  IEPT  IOFF  IAME  IAMT IRNAME
 2     1   -99   -99   -99   -99   -99   -99 -99
@I IDATE  IROP IRVAL
 2 %s%s IR008     2
 2 %s%s IR010     0
 2 %s%s IR009   150
 2 %s%s IR011    30
""".formatted(yy, pdt, yy, pdt, yy, pdt, yy, pdt, yy, pdt, yy, pdt, yy, pdt, yy, pdt));

                    // Paddy rice setting
                    /*
                    ! Irrigation Codes: IRRCOD
                    ! 1:  Furrow irrigation of specified amount (mm)
                    ! 2:  Alternating furrows; irrigation of specified amount (mm)
                    ! 3:  Flood irrigation of specified amount (mm)
                    ! 4:  Sprinkler irrigation of specified amount (mm)
                    ! 5:  Drip or trickle irrigation of specified amount (mm)
                    ! 6:  Single irrigation to specified total flood depth (mm)
                    ! 7:  Water table depth (cm)
                    ! 8:  Percolation rate (mm/d)
                    ! 9:  Bund height (mm)
                    ! 10: Puddling (Puddled if IRRCOD = 10 record is present)
                    ! 11: Maintain constant specified flood depth (mm)
                    */
                    int floodIrrigationDate = pdate + 5;
                    String irrigationYear = yy;
                    if (floodIrrigationDate>365)
                    {
                        floodIrrigationDate = floodIrrigationDate - 365;
                        irrigationYear = String.valueOf(Integer.parseInt(yy)+1);
                    }
                    if (isIrrigated)
                    {
                        snxSectionIrrigation.append(" 2 ").append(irrigationYear).append(dfDDD.format(floodIrrigationDate)).append(" IR011   100\n");   // Maintain constant specified flood depth (mm)
                    }
                    else
                    {
                        snxSectionIrrigation.append(" 2 ").append(irrigationYear).append(dfDDD.format(floodIrrigationDate)).append(" IR003   100\n");   // Flood irrigation of specified amount (mm)
                    }
                }
            }
            else
            {
                snxSectionIrrigation.append("""
@I  EFIR  IDEP  ITHR  IEPT  IOFF  IAME  IAMT IRNAME
 1     1   -99   -99   -99   -99   -99   -99 -99
@I IDATE  IROP IRVAL
 1   %s IR001   100
@I  EFIR  IDEP  ITHR  IEPT  IOFF  IAME  IAMT IRNAME
 2     1   -99   -99   -99   -99   -99   -99 -99
@I IDATE  IROP IRVAL
""".formatted(dfDDD.format(1)));

                // When to irrigate?
                TreeMap<Integer, Integer> irrigation = new TreeMap<>();
                irrigation.put(1, 10);   // Planting date

                if (daysToFlowering>10)
                {
                    irrigation.put(daysToFlowering-1, 15);
                    irrigation.put(daysToFlowering,   15);  // Also the split fertilizer application date
                    irrigation.put(daysToFlowering+3, 15);
                }

                // Additional supplementary irrigation after flowering
                for (int d=daysToFlowering+10; d<daysToHarvest-20; d=d+5)
                    irrigation.put(d, 10);

                for (Map.Entry<Integer, Integer> entry : irrigation.entrySet())
                    snxSectionIrrigation.append(" 2   ").append(dfDDD.format(entry.getKey())).append(" IR001   ").append(dfDDD.format(entry.getValue())).append("\n");

            }
        }

        // Batch file
        batch.append("TOUCAN").append(dfTT.format(threadID)).append(".SNX                                                                                     ").append(dfTT.format(tn)).append("      1      0      0      0\n");

        // Initial Conditions
        String icdat = yy+"001";
        String icbl = dfDDD.format(soilRootingDepth);
        String icwd = dfDDD.format(soilRootingDepth/2);
        String snxSectionInitialConditions = "\n*INITIAL CONDITIONS\n";
        if (isIrrigated || isRice)
        {
            snxSectionInitialConditions += """
                    @C   PCR ICDAT  ICRT  ICND  ICRN  ICRE  ICWD ICRES ICREN ICREP ICRIP ICRID ICNAME
                     1    FA %s   100     0     1     1   %s  1000    .8     0   100    15 -99
                    @C  ICBL  SH2O  SNH4  SNO3
                     1   %s  .500  .001  .001
                    """.formatted(icdat, icwd, icbl);
        }
        else
        {
            snxSectionInitialConditions += """
                    @C   PCR ICDAT  ICRT  ICND  ICRN  ICRE  ICWD ICRES ICREN ICREP ICRIP ICRID ICNAME
                     1    FA %s   100     0     1     1   001  1000    .8     0   100    15 -99
                    @C  ICBL  SH2O  SNH4  SNO3
                     1   %s  .001  .001  .001
                    """.formatted(icdat, icbl);
        }

        // Environment modifications
        String snxSectionEnvironmentModification = """

*ENVIRONMENT MODIFICATIONS
@E ODATE EDAY  ERAD  EMAX  EMIN  ERAIN ECO2  EDEW  EWIND ENVNAME  
 1 %s A 0.0 A 0.0 A   0 A   0 A 0.0 R %s A   0 A   0 
""".formatted(icdat, co2);

        // Harvest details
        int harvestDate = 365;
        if (harvestDate<1) harvestDate = 1;
        String hd = dfDDD.format(harvestDate);
        String hbpc = dfDDD.format(residueHarvestPct);
        String harvestYear = dfTT.format(Integer.parseInt(yy) + 1);
        String snxHarvest = """

*HARVEST DETAILS
@H HDATE  HSTG  HCOM HSIZE   HPC  HBPC HNAME
 1 %s%s GS000   -99   -99   100   %s
 2 %s%s GS000   -99   -99   100   %s
""".formatted(harvestYear, hd, hbpc, harvestYear, hd, hbpc);

        // Organic amendment
        String snxSectionManure = """

*RESIDUES AND ORGANIC FERTILIZER
@R RDATE  RCOD  RAMT  RESN  RESP  RESK  RINP  RDEP  RMET RENAME
 1 %s001 RE003  1000   1.4    .2  2.38    20    15 AP003 -99
""".formatted(yy);

        // Simulation controls
        String irrig = "D";  if (isRice)  irrig = "R";
        String harvs = "M";  if (isWheat) harvs = "R";
        String nyers = dfTT.format(numberOfYears);
        String snxSectionSimulationControls = """

*SIMULATION CONTROLS
@N GENERAL     NYERS NREPS START SDATE RSEED SNAME.................... SMODEL
 1 GE             %s     1     S %s  4537 CROP
@N OPTIONS     WATER NITRO SYMBI PHOSP POTAS DISES  CHEM  TILL   CO2
 1 OP              Y     Y     Y     N     N     N     N     N     D
@N METHODS     WTHER INCON LIGHT EVAPO INFIL PHOTO HYDRO NSWIT MESOM MESEV MESOL
 1 ME              G     M     E     R     S     C     R     1     P     S     2
@N MANAGEMENT  PLANT IRRIG FERTI RESID HARVS
 1 MA              R     %s     D     R     %s
@N OUTPUTS     FNAME OVVEW SUMRY FROPT GROUT CAOUT WAOUT NIOUT MIOUT DIOUT VBOSE CHOUT OPOUT FMOPT
 1 OU              N     N     Y     3     N     N     N     N     N     N     0     N     N     C
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
 1 HA              0 79065   100   %s
@N GENERAL     NYERS NREPS START SDATE RSEED SNAME.................... SMODEL
 2 GE              1     1     S %s  2150 FALLOW
@N OPTIONS     WATER NITRO SYMBI PHOSP POTAS DISES  CHEM  TILL   CO2
 2 OP              Y     Y     Y     N     N     N     N     N     D
@N METHODS     WTHER INCON LIGHT EVAPO INFIL PHOTO HYDRO NSWIT MESOM MESEV MESOL
 2 ME              G     M     E     R     S     C     R     1     P     S     2
@N MANAGEMENT  PLANT IRRIG FERTI RESID HARVS
 2 MA              R     N     N     R     R
@N OUTPUTS     FNAME OVVEW SUMRY FROPT GROUT CAOUT WAOUT NIOUT MIOUT DIOUT VBOSE CHOUT OPOUT FMOPT
 2 OU              Y     N     A     5     N     N     N     N     N     N     N     N     N     A
@  AUTOMATIC MANAGEMENT
@N PLANTING    PFRST PLAST PH2OL PH2OU PH2OD PSTMX PSTMN
 2 PL          75169 75183    40   100    30    40    10
@N IRRIGATION  IMDEP ITHRL ITHRU IROFF IMETH IRAMT IREFF
 2 IR             30    70   100 IB001 IB001    20   .75
@N NITROGEN    NMDEP NMTHR NAMNT NCODE NAOFF
 2 NI             30    50    25 IB001 IB001
@N RESIDUES    RIPCN RTIME RIDEP
 2 RE            100     1    20
@N HARVEST     HFRST HLAST HPCNP HPCNR
 2 HA              0 79065   100   %s
""".formatted(nyers, icdat, irrig, harvs, icdat, icdat, hbpc, icdat, hbpc);

        // SNX
        String snx = "*EXP.DETAILS: TOUCAN"+dfTT.format(threadID)+"SN SEASONAL RUNS\n" +
                "\n" +
                "*GENERAL\n" +
                "\n" +
                snxSectionTreatments +
                "\n*CULTIVARS\n" +
                "@C CR INGENO CNAME\n" +
                " 1 "+cultivarOption[1]+" "+cultivarOption[2]+" "+cultivarOption[3]+"\n" +
                snxSectionFieldLevel1 +
                snxSectionFieldLevel2 +
                snxSectionInitialConditions +
                snxSectionPlantingDetails +
                snxSectionFertilizer +
                snxSectionIrrigation +
                snxSectionManure +
                snxSectionEnvironmentModification +
                snxHarvest +
                snxSectionSimulationControls;

        // Write
        String snxFile = App.directoryThreads+"T"+threadID+App.d+"TOUCAN"+dfTT.format(threadID)+".SNX";
        Utility.writeFile(snxFile, snx);

        // Write
        String batchFile = App.directoryThreads+"T"+threadID+App.d+"DSSBatch.v48";
        Utility.writeFile(batchFile, batch.toString());

    }    

}
