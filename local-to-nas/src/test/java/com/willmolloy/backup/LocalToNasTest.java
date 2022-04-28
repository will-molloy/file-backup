package com.willmolloy.backup;

import static com.google.common.truth.Truth.assertThat;

import com.github.javafaker.Faker;
import com.google.common.truth.Correspondence;
import com.google.common.truth.IterableSubject;
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
    Path file1 = createFileAt(source.resolve("file1"));
    Path file2 = createFileAt(source.resolve("file2"));
    Path nestedFile1 = createFileAt(source.resolve("nested/file1"));
    Path nestedFile2 = createFileAt(source.resolve("nested/directory/file2"));
    Path emptyDirectory = createDirectoryAt(source.resolve("user/documents"));

    // When
    localToNas.backup(source, destination);

    // Then
    for (Path path : List.of(source, destination)) {
      assertThatDirectory(path)
          .containsExactly(
              path,
              file1,
              file2,
              nestedFile1.getParent(),
              nestedFile1,
              nestedFile2.getParent(),
              nestedFile2,
              emptyDirectory.getParent(),
              emptyDirectory);
    }
  }

  @Test
  void given_filesOnBothSourceAndDestination_then_replacesFilesOnDestination() {}

  @Test
  void given_filesOnlyOnDestination_then_deletesFilesOnDestination() {}

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

  private IterableSubject.UsingCorrespondence<Path, Path> assertThatDirectory(Path path)
      throws IOException {
    return assertThat(Files.walk(path).toList()).comparingElementsUsing(pathsEquivalent());
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
