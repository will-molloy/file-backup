package com.willmolloy.backup;

import static com.google.common.collect.Iterables.toArray;
import static com.google.common.truth.Truth.assertThat;

import com.github.javafaker.Faker;
import com.google.common.truth.Correspondence;
import com.willmolloy.backup.util.DirectoryWalker;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
@SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
class IntegrationTest {

  private Path testFiles;
  private Path source;
  private Path destination;

  private final Faker faker = new Faker();
  private final DirectoryWalker directoryWalker = new DirectoryWalker();

  @BeforeEach
  void setUp() throws IOException {
    testFiles = Path.of(this.getClass().getSimpleName());
    source = testFiles.resolve("source");
    destination = testFiles.resolve("destination");

    Files.createDirectories(source);
    Files.createDirectories(destination);
  }

  @AfterEach
  void tearDown() throws IOException {
    FileUtils.deleteDirectory(testFiles.toFile());
  }

  @Test
  void given_filesOnlyOnSource_then_copiesFilesToDestination() {
    // Given
    List<Path> filesUnderSource = createRandomFilesOrDirectoriesUnder(source);

    // When
    runApp(false);

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(
        toArray(filesUnderSource, Path.class));
  }

  @Test
  void given_filesOnlyOnDestination_then_deletesFilesOnDestination() {
    // Given
    List<Path> filesUnderDestination = createRandomFilesOrDirectoriesUnder(destination);

    // When
    runApp(false);

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource();
  }

  @Test
  void given_fileOnBothSourceAndDestination_then_updatesFileOnDestination() {
    // Given
    String fileName = faker.file().fileName();
    Path fileUnderSource =
        createFileAt(
            source.resolve(fileName),
            faker.lorem().paragraphs(faker.number().numberBetween(5, 10)));
    Path fileUnderDestination = createFileAt(destination.resolve(fileName), List.of());

    // When
    runApp(false);

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(fileUnderSource);
  }

  @Test
  void given_directoryOnSourceAndSubDirectoryOnDestination_then_deletesSubDirectoryOnDestination() {
    // Given
    Path directoryUnderSource = createDirectoryAt(source.resolve("user/documents"));
    Path directoryUnderDestination = createDirectoryAt(destination.resolve("user/documents/work"));

    // When
    runApp(false);

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(directoryUnderSource);
  }

  @Test
  void
      given_directoryOnSourceAndSuperDirectoryOnDestination_then_createsSubDirectoryOnDestination() {
    // Given
    Path directoryUnderSource = createDirectoryAt(source.resolve("user/documents/work"));
    Path directoryUnderDestination = createDirectoryAt(destination.resolve("user/documents"));

    // When
    runApp(false);

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(directoryUnderSource);
  }

  @Test
  void given_filesOnlyOnSource_and_dryRun_then_doesNothing() {
    // Given
    List<Path> filesUnderSource = createRandomFilesOrDirectoriesUnder(source);

    // When
    runApp(true);

    // Then
    assertThatDirectoryContainsExactly(source, toArray(filesUnderSource, Path.class));
    assertThatDirectoryContainsExactly(destination);
  }

  @Test
  void given_filesOnlyDestination_and_dryRun_then_doesNothing() {
    // Given
    List<Path> filesUnderDestination = createRandomFilesOrDirectoriesUnder(destination);

    // When
    runApp(true);

    // Then
    assertThatDirectoryContainsExactly(source);
    assertThatDirectoryContainsExactly(destination, toArray(filesUnderDestination, Path.class));
  }

  private void runApp(boolean dryRun) {
    Main.main(source.toString(), destination.toString(), Boolean.toString(dryRun));
  }

  private List<Path> createRandomFilesOrDirectoriesUnder(Path parentDirectory) {
    return IntStream.range(0, faker.number().numberBetween(5, 10))
        .mapToObj(i -> createRandomFileOrDirectoryUnder(parentDirectory))
        .toList();
  }

  private Path createRandomFileOrDirectoryUnder(Path parentDirectory) {
    if (faker.random().nextBoolean()) {
      return createRandomFileUnder(parentDirectory);
    } else {
      return createRandomDirectoryUnder(parentDirectory);
    }
  }

  private Path createRandomFileUnder(Path parentDirectory) {
    Path file = parentDirectory.resolve(faker.file().fileName());
    List<String> paragraphs = faker.lorem().paragraphs(faker.number().numberBetween(5, 10));
    return createFileAt(file, paragraphs);
  }

  private Path createFileAt(Path file, List<String> contents) {
    try {
      Files.createDirectories(file.getParent());
      Files.createFile(file);
      Files.write(file, contents);
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Path createRandomDirectoryUnder(Path parentDirectory) {
    Path directory = parentDirectory.resolve(faker.file().fileName(null, null, "", null));
    return createDirectoryAt(directory);
  }

  private Path createDirectoryAt(Path directory) {
    try {
      Files.createDirectories(directory);
      return directory;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void assertThatSourceAndDestinationContainsExactlyRelativeFromSource(
      Path... expectedLeavesRelativeFromSource) {
    assertThatDirectoryContainsExactly(source, expectedLeavesRelativeFromSource);

    List<Path> expectedLeavesRelativeFromDestination =
        Arrays.stream(expectedLeavesRelativeFromSource)
            .map(
                path -> {
                  Path relativeFromSource = source.relativize(path);
                  return destination.resolve(relativeFromSource);
                })
            .toList();
    assertThatDirectoryContainsExactly(
        destination, toArray(expectedLeavesRelativeFromDestination, Path.class));
  }

  private void assertThatDirectoryContainsExactly(Path directory, Path... expectedLeaves) {
    assertThat(directoryWalker.leavesExcludingSelf(directory).toList())
        .comparingElementsUsing(pathsEquivalent())
        .containsExactlyElementsIn(expectedLeaves);
  }

  private Correspondence<Path, Path> pathsEquivalent() {
    return Correspondence.<Path, Path>from(
            (actual, expected) -> {
              try {
                return actual.equals(expected) && Files.mismatch(actual, expected) == -1;
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            },
            "is equal to with same contents")
        .formattingDiffsUsing(
            (actual, expected) -> {
              if (!actual.equals(expected)) {
                return "paths not equal";
              }
              return "contents not same";
            });
  }
}
