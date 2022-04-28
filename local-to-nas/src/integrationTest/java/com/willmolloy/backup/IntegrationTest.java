package com.willmolloy.backup;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;

import com.github.javafaker.Faker;
import com.google.common.collect.Iterables;
import com.google.common.truth.Correspondence;
import com.willmolloy.backup.util.DirectoryWalker;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
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
  void given_fileOnlyOnSource_then_copiesFileToDestination() throws IOException {
    // Given
    Path sourceFile1 = createFileAt(source.resolve("file1"));

    // When
    Main.main(source.toString(), destination.toString(), "false");

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(sourceFile1);
  }

  @Test
  void given_filesOnlyOnSource_then_copiesFilesToDestination() throws IOException {
    // Given
    Path sourceFile1 = createFileAt(source.resolve("file1"));
    Path sourceFile2 = createFileAt(source.resolve("file2"));
    Path sourceNestedFile1 = createFileAt(source.resolve("nested/file1"));
    Path sourceNestedFile2 = createFileAt(source.resolve("nested/directory/file2"));
    Path sourceEmptyDirectory = createDirectoryAt(source.resolve("user/documents"));

    // When
    Main.main(source.toString(), destination.toString(), "false");

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(
        sourceFile1, sourceFile2, sourceNestedFile1, sourceNestedFile2, sourceEmptyDirectory);
  }

  @Test
  void given_emptyDirectoryOnlyOnSource_then_copiesEmptyDirectoryToDestination()
      throws IOException {
    // Given
    Path sourceEmptyDirectory = createDirectoryAt(source.resolve("my-directory"));

    // When
    Main.main(source.toString(), destination.toString(), "false");

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(sourceEmptyDirectory);
  }

  @Test
  void given_fileOnlyOnDestination_then_deletesFileOnDestination() throws IOException {
    // Given
    Path destinationFile1 = createFileAt(destination.resolve("file1"));

    // When
    Main.main(source.toString(), destination.toString(), "false");

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource();
  }

  @Test
  void given_filesOnlyOnDestination_then_deletesFilesOnDestination() throws IOException {
    // Given
    Path destinationFile1 = createFileAt(destination.resolve("file1"));
    Path destinationFile2 = createFileAt(destination.resolve("file2"));
    Path destinationNestedFile1 = createFileAt(destination.resolve("nested/file1"));
    Path destinationNestedFile2 = createFileAt(destination.resolve("nested/directory/file2"));
    Path destinationEmptyDirectory = createDirectoryAt(destination.resolve("user/documents"));

    // When
    Main.main(source.toString(), destination.toString(), "false");

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource();
  }

  @Test
  void given_emptyDirectoryOnlyOnDestination_then_deletesEmptyDirectoryOnDestination()
      throws IOException {
    // Given
    Path destinationEmptyDirectory = createDirectoryAt(destination.resolve("user/documents"));

    // When
    Main.main(source.toString(), destination.toString(), "false");

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource();
  }

  @Test
  void given_fileOnBothSourceAndDestination_then_replacesFileOnDestination() throws IOException {
    // Given
    Path sourceFile1 = createFileAt(source.resolve("file1"));

    Path destinationFile1 = createFileAt(destination.resolve("file1"));

    // When
    Main.main(source.toString(), destination.toString(), "false");

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(sourceFile1);
  }

  @Test
  void given_filesOnBothSourceAndDestination_then_replacesFilesOnDestination() throws IOException {
    // Given
    Path sourceFile1 = createFileAt(source.resolve("file1"));
    Path sourceFile2 = createFileAt(source.resolve("file2"));
    Path sourceNestedFile1 = createFileAt(source.resolve("nested/file1"));
    Path sourceNestedFile2 = createFileAt(source.resolve("nested/directory/file2"));
    Path sourceEmptyDirectory = createDirectoryAt(source.resolve("user/documents"));

    Path destinationFile1 = createFileAt(destination.resolve("file1"));
    Path destinationFile2 = createFileAt(destination.resolve("file2"));
    Path destinationNestedFile1 = createFileAt(destination.resolve("nested/file1"));
    Path destinationNestedFile2 = createFileAt(destination.resolve("nested/directory/file2"));
    Path destinationEmptyDirectory = createDirectoryAt(destination.resolve("user/documents"));

    // When
    Main.main(source.toString(), destination.toString(), "false");

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(
        sourceFile1, sourceFile2, sourceNestedFile1, sourceNestedFile2, sourceEmptyDirectory);
  }

  @Test
  void given_emptyDirectoryOnBothSourceAndDestination_then_replacesEmptyDirectoryOnDestination()
      throws IOException {
    // Given
    Path sourceEmptyDirectory = createDirectoryAt(source.resolve("user/documents"));

    Path destinationEmptyDirectory = createDirectoryAt(destination.resolve("user/documents"));

    // When
    Main.main(source.toString(), destination.toString(), "false");

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(sourceEmptyDirectory);
  }

  @Test
  void given_directoryOnSourceAndSubDirectoryOnDestination_then_deletesSubDirectoryOnDestination()
      throws IOException {
    // Given
    Path sourceEmptyDirectory = createDirectoryAt(source.resolve("user/documents"));

    Path destinationEmptyDirectory = createDirectoryAt(destination.resolve("user/documents/work"));

    // When
    Main.main(source.toString(), destination.toString(), "false");

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(sourceEmptyDirectory);
  }

  @Test
  void given_directoryOnSourceAndSuperDirectoryOnDestination_then_createsSubDirectoryOnDestination()
      throws IOException {
    // Given
    Path sourceEmptyDirectory = createDirectoryAt(source.resolve("user/documents/work"));

    Path destinationEmptyDirectory = createDirectoryAt(destination.resolve("user/documents"));

    // When
    Main.main(source.toString(), destination.toString(), "false");

    // Then
    assertThatSourceAndDestinationContainsExactlyRelativeFromSource(sourceEmptyDirectory);
  }

  @Test
  void given_filesOnlyOnSource_and_dryRun_then_doesNothing() throws IOException {
    // Given
    Path sourceFile1 = createFileAt(source.resolve("file1"));
    Path sourceFile2 = createFileAt(source.resolve("file2"));
    Path sourceNestedFile1 = createFileAt(source.resolve("nested/file1"));
    Path sourceNestedFile2 = createFileAt(source.resolve("nested/directory/file2"));
    Path sourceEmptyDirectory = createDirectoryAt(source.resolve("user/documents"));

    // When
    Main.main(source.toString(), destination.toString(), "true");

    // Then
    assertThatDirectoryContainsExactly(
        source,
        sourceFile1,
        sourceFile2,
        sourceNestedFile1,
        sourceNestedFile2,
        sourceEmptyDirectory);
    assertThatDirectoryContainsExactly(destination);
  }

  @Test
  void given_filesOnlyDestination_and_dryRun_then_doesNothing() throws IOException {
    // Given
    Path destinationFile1 = createFileAt(destination.resolve("file1"));
    Path destinationFile2 = createFileAt(destination.resolve("file2"));
    Path destinationNestedFile1 = createFileAt(destination.resolve("nested/file1"));
    Path destinationNestedFile2 = createFileAt(destination.resolve("nested/directory/file2"));
    Path destinationEmptyDirectory = createDirectoryAt(destination.resolve("user/documents"));

    // When
    Main.main(source.toString(), destination.toString(), "true");

    // Then
    assertThatDirectoryContainsExactly(source);
    assertThatDirectoryContainsExactly(
        destination,
        destinationFile1,
        destinationFile2,
        destinationNestedFile1,
        destinationNestedFile2,
        destinationEmptyDirectory);
  }

  private Path createFileAt(Path path) throws IOException {
    createDirectoryAt(checkNotNull(path.getParent()));
    Files.createFile(path);
    Files.writeString(path, faker.lorem().paragraph());
    return path;
  }

  private Path createDirectoryAt(Path path) throws IOException {
    Files.createDirectories(path);
    return path;
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
        destination, Iterables.toArray(expectedLeavesRelativeFromDestination, Path.class));
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
