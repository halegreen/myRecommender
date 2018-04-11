package org.shaw.myhadoop;

import java.util.concurrent.Executors;

public class Driver {
    public static void main(String[] args) throws Exception{
        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
        Normalize normalize = new Normalize();
        Multiplication multiplication = new Multiplication();
        Sum sum = new Sum();

        String rawInputDir = args[0];
        String userMovieListOutputDir = args[1];
        String coOccurrenceMarDir = args[2];
        String normalizeDir = args[3];
        String multiplyOutDir = args[4];
        String sumOutDir = args[5];

        String[] path0 = {rawInputDir, userMovieListOutputDir};
        String[] path1 = {userMovieListOutputDir, coOccurrenceMarDir};
        String[] path2 = {coOccurrenceMarDir, normalizeDir};
        String[] path3 = {normalizeDir, rawInputDir, multiplyOutDir};
        String[] path4 = {multiplyOutDir, sumOutDir};

        dataDividerByUser.main(path0);
        coOccurrenceMatrixGenerator.main(path1);
        normalize.main(path2);
        multiplication.main(path3);
//        sum.main(path4);
    }
}
