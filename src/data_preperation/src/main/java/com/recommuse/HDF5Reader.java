package com.recommuse;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import java.io.File;
import java.util.*;
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

      // Read musicbrainz data - handle as LinkedHashMap instead of Map[]
      Dataset musicbrainzSongsDataset = hdfFile.getDatasetByPath("musicbrainz/songs");
      LinkedHashMap < String, Object > musicbrainzSong = (LinkedHashMap < String, Object > ) musicbrainzSongsDataset.getData();

      // Read array datasets with null checks
      List < Double > segmentsPitches = extract2DStats(safeRead2DDoubleList(hdfFile, "analysis/segments_pitches"));
      List < Double > segmentsTimbre = extract2DStats(safeRead2DDoubleList(hdfFile, "analysis/segments_timbre"));

      List < Double > timbreCov = extractCovariance(safeRead2DDoubleList(hdfFile, "analysis/segments_timbre"));
      List < Double > pitchCov = extractCovariance(safeRead2DDoubleList(hdfFile, "analysis/segments_pitches"));

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
        .setRelease(safeGetString(metadataSong, "release"))
        .setArtistFamiliarity(safeGetDouble(metadataSong, "artist_familiarity"))
        .setArtistHotttnesss(safeGetDouble(metadataSong, "artist_hotttnesss"))
        .setSongHotttnesss(safeGetDouble(metadataSong, "song_hotttnesss"))
        .setDuration(safeGetDouble(analysisSong, "duration"))
        .setKey(safeGetInt(analysisSong, "key"))
        .setTempo(safeGetDouble(analysisSong, "tempo"))
        .setMode(safeGetInt(analysisSong, "mode"))
        .setLoudness(safeGetDouble(analysisSong, "loudness"))
        .setTimeSignature(safeGetInt(analysisSong, "time_signature"))
        .setKeyConfidence(safeGetDouble(analysisSong, "key_confidence"))
        .setModeConfidence(safeGetDouble(analysisSong, "mode_confidence"))
        .setSegmentsPitches(segmentsPitches)
        .setSegmentsTimbre(segmentsTimbre)
        .setTimbreCov(timbreCov)
        .setPitchCov(pitchCov)
        .setYear(safeGetInt(musicbrainzSong, "year"))
        .build();
    } catch (Exception e) {
      throw new RuntimeException("Error reading HDF5 file: " + hdf5Path, e);
    }
  }

  private static List < Double > extract1DStats(List < Double > array) {
    if (array == null || array.isEmpty()) {
      return Arrays.asList(0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    }

    double[] primitive = array.stream().mapToDouble(Double::doubleValue).toArray();

    // Calculate statistics
    double mean = Arrays.stream(primitive).average().orElse(0.0);
    double median = calculateMedian(primitive);
    double min = Arrays.stream(primitive).min().orElse(0.0);
    double max = Arrays.stream(primitive).max().orElse(0.0);
    double std = Math.sqrt(Arrays.stream(primitive)
      .map(v -> Math.pow(v - mean, 2))
      .average().orElse(0.0));
    double count = (double) array.size();

    return Arrays.asList(mean, median, min, max, std, count);
  }

  private static List < Double > extract2DStats(List < List < Double >> array2D) {
    if (array2D == null || array2D.isEmpty()) {
      return Collections.nCopies(72, 0.0); // 12 dimensions × 6 stats = 72
    }

    List < Double > stats = new ArrayList < > ();

    // For each of the 12 dimensions, extract 6 statistics
    for (int dim = 0; dim < 12; dim++) {
      final int dimension = dim;

      // Extract values for this dimension across all segments
      double[] dimValues = array2D.stream()
        .mapToDouble(segment -> {
          if (segment != null && segment.size() > dimension) {
            return segment.get(dimension);
          }
          return 0.0;
        })
        .toArray();

      // Convert to list and extract statistics
      List < Double > dimList = DoubleStream.of(dimValues).boxed().collect(Collectors.toList());
      stats.addAll(extract1DStats(dimList));
    }

    return stats;
  }

  private static double calculateMedian(double[] values) {
    if (values.length == 0) return 0.0;

    double[] sorted = values.clone();
    Arrays.sort(sorted);

    int middle = sorted.length / 2;
    if (sorted.length % 2 == 0) {
      return (sorted[middle - 1] + sorted[middle]) / 2.0;
    } else {
      return sorted[middle];
    }
  }

  private static List < Double > extractCovariance(List < List < Double >> matrix2D) {
    if (matrix2D == null || matrix2D.isEmpty()) {
      return Collections.nCopies(78, 0.0);
    }

    int rows = matrix2D.size();
    int cols = 12; // timbre & pitches have 12 dimensions
    double[][] data = new double[cols][rows];

    // transpose to column-major (each row is one segment, each column is one dim)
    for (int seg = 0; seg < rows; seg++) {
      List < Double > row = matrix2D.get(seg);
      for (int dim = 0; dim < cols && dim < row.size(); dim++) {
        data[dim][seg] = row.get(dim);
      }
    }

    // compute covariance
    double[][] cov = covarianceMatrix(data);

    // flatten upper-triangle
    List < Double > result = new ArrayList < > (78);
    for (int i = 0; i < 12; i++) {
      for (int j = i; j < 12; j++) {
        result.add(cov[i][j]);
      }
    }
    return result;
  }

  private static double[][] covarianceMatrix(double[][] columnMajor) {
    int nDims = columnMajor.length;
    int nObs = columnMajor[0].length;
    double[][] cov = new double[nDims][nDims];

    double[] mean = new double[nDims];
    for (int i = 0; i < nDims; i++) {
      double sum = 0.0;
      for (int j = 0; j < nObs; j++) sum += columnMajor[i][j];
      mean[i] = sum / nObs;
    }

    for (int i = 0; i < nDims; i++) {
      for (int j = i; j < nDims; j++) {
        double sum = 0.0;
        for (int k = 0; k < nObs; k++) {
          sum += (columnMajor[i][k] - mean[i]) * (columnMajor[j][k] - mean[j]);
        }
        cov[i][j] = sum / (nObs - 1);
        cov[j][i] = cov[i][j]; // symmetric
      }
    }
    return cov;
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
