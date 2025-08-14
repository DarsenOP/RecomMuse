package com.recommuse;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
  public static void main(String[] args) {
    if (args.length == 1) {
      AvroReader.readAvro(args[0]);
      System.exit(0);
    }
    
    if (args.length != 2) {
      System.err.println("Usages: ");
      System.err.println("\t1. java -jar your-jar.jar <input-directory> <output-directory>");
      System.err.println("\t2. java -jar your-jar.jar <input-avro-file>");
      System.exit(1);
    }

    String inputDir = args[0];
    String outputDir = args[1];

    try {
      // Create output directory if it doesn't exist
      Files.createDirectories(Paths.get(outputDir));

      // Initialize Avro writer
      DatumWriter < RecomMuse > datumWriter = new SpecificDatumWriter < > (RecomMuse.class);
      DataFileWriter < RecomMuse > dataFileWriter = new DataFileWriter < > (datumWriter);

      // Create a single Avro file for all records
      File outputFile = new File(outputDir, "music_data.avro");
      DataFileWriter < RecomMuse > writer = dataFileWriter.create(RecomMuse.getClassSchema(), outputFile);

      // Process each HDF5 file in the input directory
      Files.list(Paths.get(inputDir))
        .filter(path -> path.toString().endsWith(".h5"))
        .forEach(path -> {
          try {
            System.out.println("Processing: " + path);
            RecomMuse record = HDF5Reader.readHDF5(path.toString());
            writer.append(record);
          } catch (Exception e) {
            System.err.println("Error processing file: " + path);
            e.printStackTrace();
          }
        });

      writer.close();
      System.out.println("Successfully converted all files to: " + outputFile.getAbsolutePath());
    } catch (IOException e) {
      System.err.println("Error during conversion:");
      e.printStackTrace();
      System.exit(1);
    }
  }
}
