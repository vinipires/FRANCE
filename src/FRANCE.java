
import ch.ethz.globis.pht.PhTreeF;

import java.io.*;

import java.util.Scanner;





/**
 * Created by vini on 01/04/16.
 */
public class FRANCE {


    long qtyObjs = 0;


    public FRANCE(File inputFile, int qtyBuckets) throws IOException {

        long objsPerBucket;
        long qtyIntervalL, qtyIntervalR;
        int epsilon, bucket, sumObjsL, sumObjsR, i;
        long[] qtyInterval;
        long qtyInter, offsetL, offsetR,  maxInter, errorL, errorR;
        double startRAL, endRAL, startRAR, endRAR, diffL, diffR;
        double[] limits = new double[qtyBuckets-1];


        if(qtyBuckets <= 0) error(2, "Not valid quantity of buckets.");

        // Read File and store it in PHTree. The key is RA and the value is a array of Strings.
        PhTreeF<String[]> phtree =  readFile(inputFile);


        if(qtyBuckets > qtyObjs) error(3, "There are more buckets than objects.");

        objsPerBucket = qtyObjs/ qtyBuckets;

        // Dynamic error
        epsilon = (int) Math.abs(objsPerBucket * 0.005);   // Let's say that 0,5% of the objsPerBucket is acceptable


        maxInter = 50;  // Just to make sure we don't overrun


        endRAL = 0.0;
        endRAR = 360.0;
        offsetL = 0;
        offsetR = 0;
        sumObjsL = 0;
        sumObjsR = 0;

        for(bucket=1; bucket < ((float)qtyBuckets/2.0); bucket++) { // While there is at least one bucket to do

            System.err.printf("\rRunning FRANCE over partitions... [%d%%]",  (int)((bucket-1) * 100/(int)(qtyBuckets/2)));


            startRAL = endRAL;
            startRAR = endRAR;
            endRAL = endRAR = startRAL + (startRAR - startRAL)/2;
            diffL = endRAL - startRAL;
            diffR = startRAR - endRAR;

            qtyInter = 1;
            qtyInterval = countBetween2(phtree, startRAL, endRAL, startRAR, endRAR);

            qtyIntervalL = qtyInterval[0];
            qtyIntervalR = qtyInterval[1];

            errorL = Math.abs(qtyIntervalL -(objsPerBucket-offsetL));
            errorR = Math.abs(qtyIntervalR -(objsPerBucket-offsetR));



            while(errorL  > epsilon || errorR > epsilon) {
                //printf("[Inter #%2d] startRAL: %.5f  endRAL: %.5f qtyIntervalL: %d\n", qtyInter, startRAL, endRAL, qtyIntervalL);
                //printf("[Inter #%2d] startRAR: %.5f  endRAR: %.5f qtyIntervalR: %d\n", qtyInter, startRAR, endRAR, qtyIntervalR);
                diffL /= 2;
                diffR /= 2;
                if(qtyIntervalL < objsPerBucket) endRAL += diffL;
                else  endRAL -= diffL;
                if(qtyIntervalR < objsPerBucket) endRAR -= diffR;
                else  endRAR += diffR;
                qtyInter++;
                if(qtyInter >= maxInter) break;

                qtyInterval = countBetween2(phtree, startRAL, endRAL, startRAR, endRAR);

                qtyIntervalL = qtyInterval[0];
                qtyIntervalR = qtyInterval[1];


                errorL = Math.abs(qtyIntervalL -(objsPerBucket-offsetL));
                errorR = Math.abs(qtyIntervalR -(objsPerBucket-offsetR));
            }

            limits[bucket-1] = endRAL;
            limits[qtyBuckets-bucket-1] = endRAR;

            sumObjsL += qtyIntervalL;
            sumObjsR += qtyIntervalR;
            offsetL = sumObjsL - bucket*objsPerBucket;
            offsetR = sumObjsR - bucket*objsPerBucket;

        }


        if(qtyBuckets % 2 == 0) { // if qtyBuckets is even, there is two buckets left - let's FRANCE the first
            System.err.printf("\rRunning FRANCE over partitions... [%d%%]",  (int)((bucket-1) * 100/(int)(qtyBuckets/2)));
            startRAL = endRAL;
            endRAL = endRAR;
            diffL = endRAL - startRAL;

            qtyInter = 1;
            errorL = Math.abs((qtyIntervalL = countBetween(phtree, startRAL, endRAL))-(objsPerBucket-offsetL));
            while(errorL  > epsilon) {
                //printf("[Inter #%2d] startRAL: %.5f  endRAL: %.5f Objects in interval: %d\n", qtyInter, startRAL, endRAL, qtyIntervalL);
                diffL /= 2;
                if(qtyIntervalL < objsPerBucket) endRAL += diffL;
                else  endRAL -= diffL;
                qtyInter++;
                if(qtyInter >= maxInter) break;
                errorL = Math.abs((qtyIntervalL = countBetween(phtree, startRAL, endRAL))-(objsPerBucket-offsetL));
            }

            /*if(verbose)
                printf("startRA: %.5f  endRA: %.5f  qtyInterval: %d  qtyInter: %d  desired: %d\n", startRAL, endRAL, qtyIntervalL, qtyInter, objsPerBucket-offsetL);*/

            limits[bucket-1] = endRAL;

            sumObjsL += qtyIntervalL;
            offsetL = sumObjsL - bucket*objsPerBucket;   // dynamic offset correction - avoid error propagation
        }

        /*if(verbose) {
            startRAL = endRAL;
            endRAL = endRAR;
            qtyIntervalL = countBetween(inputFile, startRAL, endRAL);
            printf("startRA: %.5f  endRA: %.5f  qtyInterval: %d \n", startRAL, endRAL, qtyIntervalL);
        }*/

        System.err.printf("\rRunning FRANCE over partitions... [done]\n");

        writeLimitsFile(limits,inputFile,qtyBuckets);



    }

    private void writeLimitsFile(double[] limits, File inputFile, int qtyBuckets) {



        String dataset = inputFile.getAbsolutePath();
        String[] datasetSplitted = dataset.split("/");
        String datasetPath = dataset.substring(0, (dataset.length() - datasetSplitted[datasetSplitted.length - 1].length()));

        //String fileOut = "partitions/partitions_" + qtyBuckets + "_" + datasetSplitted[datasetSplitted.length - 1];

        System.out.println("datasetPath = " + datasetPath);

        //File partitionsFolder = new File(datasetPath + "/Dataset/partitions");

        //if (!partitionsFolder.exists()) {
        //    partitionsFolder.mkdirs();
        //}

        //File output = new File(datasetPath+fileOut);

        String fileOut = "partitions_" + qtyBuckets + "_" + datasetSplitted[datasetSplitted.length - 1];
        File output = new File(fileOut);

        try( FileWriter fw = new FileWriter(output) ){

            for(int i = 0; i < qtyBuckets-1; i++) {
                fw.write(String.valueOf(limits[i]).replace(",",".")+"\n");
                fw.flush();
            }

        }catch(IOException ex){
            ex.printStackTrace();
        }

        System.out.println("Partitions file created in  = " + output);













    }


    private long[] countBetween2(PhTreeF<String[]> phtree, double startRAL, double endRAL, double startRAR,
                                            double endRAR) {

        
        long[] qtyInterval = new long[2];



        long qtyIntervalL = 0;
        long qtyIntervalR = 0;

        double[] startRALpht = new double[1];
        startRALpht[0] = startRAL;

        double[] endRALpht = new double[1];
        endRALpht[0] = endRAL;

        double[] startRARpht = new double[1];
        startRARpht[0] = startRAR;

        double[] endRARpht = new double[1];
        endRARpht[0] = endRAR;


        //System.out.println("phtree.queryAll(startRALpht,endRALpht) = " + phtree.queryAll(startRALpht,endRALpht).size());
        PhTreeF.PhQueryF<String[]> ras = phtree.query(startRALpht, endRALpht);
        while (ras.hasNext()){
            qtyIntervalL += ras.next().length;
        }


        //System.out.println("phtree.queryAll(startRARpht,endRARpht) = " + phtree.queryAll(endRARpht, startRARpht).size());
        PhTreeF.PhQueryF<String[]> ras2 = phtree.query(endRARpht, startRARpht);
        while (ras2.hasNext()){
            qtyIntervalR += ras2.next().length;
        }

        qtyInterval[0] = qtyIntervalL;
        qtyInterval[1] = qtyIntervalR;



        //System.out.printf("startRAL = %f, endRAL = %f, startRAR = %f, endRAR = %f \n", startRAL, endRAL, startRAR, endRAR);

        //System.out.printf("qtyIntervalL = %d, qtyIntervalR = %d \n", qtyIntervalL, qtyIntervalR);
        return qtyInterval;
    }


    private long countBetween(PhTreeF<String[]> phtree, double startRAL, double endRAL) {

        long qtyObjs = 0;


        double[] startRALpht = new double[1];
        startRALpht[0] = startRAL;

        double[] endRALpht = new double[1];
        endRALpht[0] = endRAL;

        PhTreeF.PhQueryF<String[]> ras = phtree.query(startRALpht, endRALpht);
        while (ras.hasNext()){
            qtyObjs += ras.next().length;
        }

        return qtyObjs;
    }






    private PhTreeF<String[]> readFile(File inputFile) throws IOException {

        PhTreeF<String[]> phtree = PhTreeF.create(1);



        double[] ra = new double[1];
        String[] lineSplitted;

        FileInputStream inputStream = null;
        Scanner sc = null;
        try {
            inputStream = new FileInputStream(inputFile);
            sc = new Scanner(inputStream, "UTF-8");
            while (sc.hasNextLine()) {
                qtyObjs++;
                String line = sc.nextLine();
                lineSplitted = line.split(";");


                ra[0] = Double.valueOf(lineSplitted[1]);
                //System.out.println("ra[0] = " + ra[0]);


                if (phtree.contains(ra)){

                    String[] arrayLines = phtree.get(ra);
                    phtree.remove(ra);
                    int tamanhoAntigoArray = arrayLines.length;
                    String[] novoArrayLines = new String[tamanhoAntigoArray+1];
                    for (int i = 0; i < tamanhoAntigoArray; i++) {
                        novoArrayLines[i] = arrayLines[i];
                    }

                    novoArrayLines[tamanhoAntigoArray] = line;
                    phtree.put(ra,novoArrayLines);

                }else{

                    String[] arrayLines = new String[1];
                    arrayLines[0] = line;
                    phtree.put(ra,arrayLines);

                }


                // System.out.println(line);
            }
            // note that Scanner suppresses exceptions

        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (sc != null) {
                sc.close();
            }
        }

        return phtree;
    }


    private void error(int value, String msg) {
        System.err.printf("Error: %s (#%d)\n", msg, value);
        System.exit(value);
    }


    public static void main(String[] args) {

        // Chech for parameters
        if (args.length < 2) {
            System.out.printf("  __  __   __         __  __   FRAgmeNtador de Catalogos Espaciais v1.0\n");
            System.out.printf(" |__ |__| |__| |\\ |  |   |__   (Space Catalogues Fragmenter)\n");
            System.out.printf(" |   |\\   |  | | \\|  |__ |__   Daniel Gaspar / Vinícius Pires\n");
            System.out.printf("                               DEXL Lab - LNCC\n");
            System.out.printf(" USAGE:\n\n INPUT_FILE QTY_OF_BUCKETS [-v]\n\n");
        } else {

            File inputFile = new File(args[0]);





            int qtyBuckets = Integer.valueOf(args[1]);



            try {
                new FRANCE(inputFile,qtyBuckets);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
