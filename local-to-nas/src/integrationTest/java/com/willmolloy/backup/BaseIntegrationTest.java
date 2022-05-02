package com.willmolloy.backup;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.io.Files.getNameWithoutExtension;
import static com.google.common.truth.Truth.assertThat;

import com.github.javafaker.Faker;
import com.google.common.truth.Correspondence;
import com.willmolloy.backup.util.DirectoryWalker;
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

/**
 * TestHelpers.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
abstract class BaseIntegrationTest {

  private Path testFiles;
  protected Path source;
  protected Path destination;

  protected final Faker faker = new Faker();
  private final DirectoryWalker directoryWalker = new DirectoryWalker();

  @BeforeEach
  void setUp() throws IOException {
    testFiles = Path.of("test-data").resolve(this.getClass().getSimpleName());
    source = testFiles.resolve("source");
    destination = testFiles.resolve("destination");

    Files.createDirectories(source);
    Files.createDirectories(destination);
  }

  @AfterEach
  void tearDown() throws IOException {
    FileUtils.deleteDirectory(testFiles.toFile());
  }

  protected void runApp(boolean dryRun) {
    Main.main(source.toString(), destination.toString(), Boolean.toString(dryRun));
  }

  protected List<Path> createRandomFilesOrDirectoriesUnder(Path parentDirectory) {
    return IntStream.range(0, faker.number().numberBetween(5, 10))
        .mapToObj(i -> createRandomFileOrDirectoryUnder(parentDirectory))
        .toList();
  }

  protected Path createRandomFileOrDirectoryUnder(Path parentDirectory) {
    if (faker.random().nextBoolean()) {
      return createRandomFileUnder(parentDirectory);
    } else {
      return createRandomDirectoryUnder(parentDirectory);
    }
  }

  protected Path createRandomFileUnder(Path parentDirectory) {
    Path file = parentDirectory.resolve(faker.file().fileName());
    List<String> paragraphs = faker.lorem().paragraphs(faker.number().numberBetween(5, 10));
    return createFileAt(file, paragraphs);
  }

  protected Path createFileAt(Path file, List<String> contents) {
    try {
      Files.createDirectories(checkNotNull(file.getParent()));
      Files.createFile(file);
      Files.write(file, contents);
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected Path createRandomDirectoryUnder(Path parentDirectory) {
    Path directory = parentDirectory.resolve(getNameWithoutExtension(faker.file().fileName()));
    return createDirectoryAt(directory);
  }

  protected Path createDirectoryAt(Path directory) {
    try {
      Files.createDirectories(directory);
      return directory;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected void assertThatSourceAndDestinationContainsExactlyRelativeFromSource(
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

  protected void assertThatDirectoryContainsExactly(Path directory, Path... expectedLeaves) {
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
