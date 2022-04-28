package com.willmolloy.backup;

import static com.google.common.truth.Truth.assertThat;

import com.github.javafaker.Faker;
import com.google.common.truth.Correspondence;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * LocalToNasTest.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
class LocalToNasTest {

  private Path testFiles;
  private Path source;
  private Path destination;

  private final Faker faker = new Faker();

  private final LocalToNas localToNas = new LocalToNas();

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
  void given_filesOnlyOnSource_then_copiesFilesToDestination() throws IOException {
    // Given
    Path sourceFile1 = createFileAt(source.resolve("file1"));
    Path sourceFile2 = createFileAt(source.resolve("file2"));
    Path sourceNestedFile1 = createFileAt(source.resolve("nested/file1"));
    Path sourceNestedFile2 = createFileAt(source.resolve("nested/directory/file2"));
    Path sourceEmptyDirectory = createDirectoryAt(source.resolve("user/documents"));

    // When
    localToNas.backup(source, destination);

    // Then
    assertThatSourceAndDestinationContainExactly(
        sourceFile1, sourceFile2, sourceNestedFile1, sourceNestedFile2, sourceEmptyDirectory);
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
    localToNas.backup(source, destination);

    // Then
    assertThatSourceAndDestinationContainExactly(
        sourceFile1, sourceFile2, sourceNestedFile1, sourceNestedFile2, sourceEmptyDirectory);
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
    localToNas.backup(source, destination);

    // Then
    assertThatSourceAndDestinationContainExactly();
  }

  private Path createFileAt(Path path) throws IOException {
    createDirectoryAt(path.getParent());
    Files.createFile(path);

    String paragraph = faker.lorem().paragraph();
    Files.writeString(path, paragraph);

    return path;
  }

  private Path createDirectoryAt(Path path) throws IOException {
    Files.createDirectories(path);
    return path;
  }

  private void assertThatSourceAndDestinationContainExactly(Path... expected) throws IOException {
    for (Path directory : List.of(source, destination)) {
      List<Path> leaves =
          Files.walk(directory)
              .filter(
                  path -> {
                    try {
                      return Files.isRegularFile(path) || Files.list(path).findAny().isEmpty();
                    } catch (IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  })
              .toList();

      assertThat(leaves).comparingElementsUsing(pathsEquivalent()).containsExactly(expected);
    }
  }

  private Correspondence<Path, Path> pathsEquivalent() {
    Correspondence.BinaryPredicate<Path, Path> recordsEquivalent =
        (actual, expected) -> {
          try {
            return actual.equals(expected) && Files.mismatch(actual, expected) == -1;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };

    Correspondence.DiffFormatter<Path, Path> diffFormatter =
        (actual, expected) -> {
          if (!actual.equals(expected)) {
            return "paths not equal";
          }
          return "contents not same";
        };

    return Correspondence.from(recordsEquivalent, "is equal to with same contents")
        .formattingDiffsUsing(diffFormatter);
  }
}
