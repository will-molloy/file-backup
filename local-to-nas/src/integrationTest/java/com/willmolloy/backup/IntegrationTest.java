package com.willmolloy.backup;

import static com.google.common.collect.Iterables.toArray;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Integration tests.
 *
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
@SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
class IntegrationTest extends BaseIntegrationTest {

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
}
