package com.recommuse;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

public class HDF5Reader {
  public static RecomMuse readHDF5(String hdf5Path) {
    try (HdfFile hdfFile = new HdfFile(new File(hdf5Path))) {
      // Read metadata - handle as LinkedHashMap instead of Map[]
      Dataset metadataSongsDataset = hdfFile.getDatasetByPath("metadata/songs");
      LinkedHashMap < String, Object > metadataSong = (LinkedHashMap < String, Object > ) metadataSongsDataset.getData();

      // Read analysis data - handle as LinkedHashMap instead of Map[]
      Dataset analysisSongsDataset = hdfFile.getDatasetByPath("analysis/songs");
      LinkedHashMap < String, Object > analysisSong = (LinkedHashMap < String, Object > ) analysisSongsDataset.getData();

      // Read musicbrainz data if available
      Dataset musicbrainzSongsDataset = hdfFile.getDatasetByPath("musicbrainz/songs");
      LinkedHashMap < String, Object > musicbrainzSong = (LinkedHashMap < String, Object > ) musicbrainzSongsDataset.getData();

      // Read array datasets with null checks
      List < Double > barsStart = safeReadDoubleList(hdfFile, "analysis/bars_start");
      List < Double > beatsStart = safeReadDoubleList(hdfFile, "analysis/beats_start");
      List < Double > segmentsStart = safeReadDoubleList(hdfFile, "analysis/segments_start");
      List < Double > segmentsLoudnessMax = safeReadDoubleList(hdfFile, "analysis/segments_loudness_max");

      List < List < Double >> segmentsPitches = safeRead2DDoubleList(hdfFile, "analysis/segments_pitches");
      List < List < Double >> segmentsTimbre = safeRead2DDoubleList(hdfFile, "analysis/segments_timbre");

      // Build Avro object
      return RecomMuse.newBuilder()
        .setArtistId(safeGetString(metadataSong, "artist_id"))
        .setArtist7digitalid(safeGetInt(metadataSong, "artist_7digitalid"))
        .setArtistMbid(safeGetString(metadataSong, "artist_mbid"))
        .setArtistPlaymeid(safeGetInt(metadataSong, "artist_playmeid"))
        .setRelease7digitalid(safeGetInt(metadataSong, "release_7digitalid"))
        .setTrack7digitalid(safeGetInt(metadataSong, "track_7digitalid"))
        .setSongId(safeGetString(metadataSong, "song_id"))
        .setTrackId(safeGetString(analysisSong, "track_id"))
        .setTitle(safeGetString(metadataSong, "title"))
        .setArtistName(safeGetString(metadataSong, "artist_name"))
        .setGenre(safeGetString(metadataSong, "genre"))
        .setRelease(safeGetString(metadataSong, "release"))
        .setArtistFamiliarity(safeGetDouble(metadataSong, "artist_familiarity"))
        .setArtistHotttnesss(safeGetDouble(metadataSong, "artist_hotttnesss"))
        .setSongHotttnesss(safeGetDouble(metadataSong, "song_hotttnesss"))
        .setDanceability(safeGetDouble(analysisSong, "danceability"))
        .setDuration(safeGetDouble(analysisSong, "duration"))
        .setEnergy(safeGetDouble(analysisSong, "energy"))
        .setKey(safeGetInt(analysisSong, "key"))
        .setTempo(safeGetDouble(analysisSong, "tempo"))
        .setMode(safeGetInt(analysisSong, "mode"))
        .setLoudness(safeGetDouble(analysisSong, "loudness"))
        .setTimeSignature(safeGetInt(analysisSong, "time_signature"))
        .setKeyConfidence(safeGetDouble(analysisSong, "key_confidence"))
        .setModeConfidence(safeGetDouble(analysisSong, "mode_confidence"))
        .setBarsStart(barsStart)
        .setBeatsStart(beatsStart)
        .setSegmentsStart(segmentsStart)
        .setSegmentsLoudnessMax(segmentsLoudnessMax)
        .setSegmentsPitches(segmentsPitches)
        .setSegmentsTimbre(segmentsTimbre)
        .setYear(safeGetInt(musicbrainzSong, "year"))
        .build();
    } catch (Exception e) {
      throw new RuntimeException("Error reading HDF5 file: " + hdf5Path, e);
    }
  }

  private static String safeGetString(Map < String, Object > map, String key) {
    Object value = map.get(key);
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return (String) value;
    }
    if (value instanceof String[]) {
      String[] array = (String[]) value;
      return array.length > 0 ? array[0] : null;
    }
    
    return value.toString();
  }

  private static List < Double > safeReadDoubleList(HdfFile hdfFile, String path) {
    try {
      Dataset dataset = hdfFile.getDatasetByPath(path);
      if (dataset == null) return null;
      double[] array = (double[]) dataset.getData();
      return array == null ? null : DoubleStream.of(array).boxed().collect(Collectors.toList());
    } catch (Exception e) {
      return null;
    }
  }

  private static List < List < Double >> safeRead2DDoubleList(HdfFile hdfFile, String path) {
    try {
      Dataset dataset = hdfFile.getDatasetByPath(path);
      if (dataset == null) return null;
      double[][] array = (double[][]) dataset.getData();
      if (array == null) return null;

      List < List < Double >> result = new ArrayList < > ();
      for (double[] innerArray: array) {
        result.add(DoubleStream.of(innerArray).boxed().collect(Collectors.toList()));
      }
      return result;
    } catch (Exception e) {
      return null;
    }
  }

  private static Integer safeGetInt(Map < String, Object > map, String key) {
    Object value = map.get(key);
    if (value instanceof Integer) {
      return ((Integer) value).intValue();
    }
    if (value instanceof int[]) {
        int[] array = (int[]) value;
        return array.length > 0 ? (Integer) array[0] : null;
    }

    return null;
  }

  private static Double safeGetDouble(Map < String, Object > map, String key) {
    Object value = map.get(key);
    if (value instanceof Double) {
      return ((Double) value).doubleValue();
    }
    if (value instanceof double[]) {
      double[] array = (double[]) value;
      return array.length > 0 ? (Double) array[0] : null;
    }

    return null;
  }
}
