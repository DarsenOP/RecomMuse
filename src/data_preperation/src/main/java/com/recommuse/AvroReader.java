package com.recommuse;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import java.io.File;

public class AvroReader {
  public static void readAvro(String avroPath) {
    File file = new File(avroPath);
    DatumReader < RecomMuse > datumReader = new SpecificDatumReader < > (RecomMuse.class);

    try (DataFileReader < RecomMuse > dataFileReader = new DataFileReader < > (file, datumReader)) {
      System.out.println("Schema: " + dataFileReader.getSchema());

      int recordCount = 0;
      while (dataFileReader.hasNext()) {
        RecomMuse record = dataFileReader.next();
        System.out.println("Record #" + (++recordCount));

        // Sample verification - check if required fields exist
        // System.out.println(record.getArtistId());
        // System.out.println(record.getArtist7digitalid());
        // System.out.println(record.getArtistMbid());
        // System.out.println(record.getArtistPlaymeid());
        // System.out.println(record.getRelease7digitalid());
        // System.out.println(record.getTrack7digitalid());
        // System.out.println(record.getSongId());
        // System.out.println(record.getTrackId());
        //
        // System.out.println(record.getTitle());
        // System.out.println(record.getArtistName());
        // System.out.println(record.getRelease());
        // System.out.println(record.getArtistFamiliarity());
        // System.out.println(record.getArtistHotttnesss());
        // System.out.println(record.getSongHotttnesss());
        //
        // System.out.println(record.getDuration());
        // System.out.println(record.getKey());
        // System.out.println(record.getTempo());
        // System.out.println(record.getMode());
        // System.out.println(record.getLoudness());
        // System.out.println(record.getTimeSignature());
        // System.out.println(record.getKeyConfidence());
        // System.out.println(record.getModeConfidence());
        //
        // System.out.println(record.getSegmentsPitches());
        // System.out.println(record.getSegmentsTimbre());
        // System.out.println(record.getPitchCov());
        // System.out.println(record.getTimbreCov());
        //
        // System.out.println(record.etYear());

        // if (record.getYear() == 0) {
        //   System.err.println("WARNING: Missing Year in record " + recordCount);
        // }
      }
      
      System.out.println("Records found: " + recordCount);
    } catch (Exception e) {
      throw new RuntimeException("Error reading Avro file: " + avroPath, e);
    }
  }
}
