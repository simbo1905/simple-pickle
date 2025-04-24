// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.simple_pickle;

import io.github.simbo1905.simple_pickle.animal.*;
import io.github.simbo1905.simple_pickle.tree.InternalNode;
import io.github.simbo1905.simple_pickle.tree.LeafNode;
import io.github.simbo1905.simple_pickle.tree.RootNode;
import io.github.simbo1905.simple_pickle.tree.TreeNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.github.simbo1905.simple_pickle.Pickler.PicklerBase.LOGGER;
import static io.github.simbo1905.simple_pickle.Pickler.picklerForSealedTrait;
import static org.junit.jupiter.api.Assertions.*;

class MorePicklerTests {
  @BeforeAll
  static void setupLogging() {
    final var logLevel = System.getProperty("java.util.logging.ConsoleHandler.level", "WARNING");
    final Level level = Level.parse(logLevel);

    LOGGER.setLevel(level);
    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(level);
    LOGGER.addHandler(consoleHandler);

    // Configure SessionKeyManager logger
    Logger logger = Logger.getLogger(Pickler.class.getName());
    logger.setLevel(level);
    ConsoleHandler skmHandler = new ConsoleHandler();
    skmHandler.setLevel(level);
    logger.addHandler(skmHandler);

    // Optionally disable parent handlers if needed
    LOGGER.setUseParentHandlers(false);
    logger.setUseParentHandlers(false);

    LOGGER.info("Logging initialized at level: " + level);
  }

  /// Tests serialization and deserialization of all animal types in a single buffer
  @Test
  void allAnimalsInBuffer() {
    // Create instances of all animal types
    final var dog = new Dog("Buddy", 3);
    final var cat = new Cat("Whiskers", true);
    final var eagle = new Eagle(2.1);
    final var penguin = new Penguin(true);
    final var alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

    // Create an array of all animals
    final var originalAnimals = new Animal[]{dog, cat, eagle, penguin, alicorn};

    // Get a pickler for the Animal sealed interface
    final var pickler = picklerForSealedTrait(Animal.class);

    // Calculate total buffer size needed using streams
    final var totalSize = Arrays.stream(originalAnimals).mapToInt(pickler::sizeOf).sum();

    // Allocate a single buffer to hold all animals
    final var buffer = ByteBuffer.allocate(totalSize);

    // Serialize all animals into the buffer using streams
    Arrays.stream(originalAnimals).forEach(animal -> pickler.serialize(animal, buffer));

    // Prepare buffer for reading
    buffer.flip();

    // Deserialize all animals from the buffer
    final var deserializedAnimals = new Animal[originalAnimals.length];
    Arrays.setAll(deserializedAnimals, ignored -> pickler.deserialize(buffer));

    // Verify all animals were correctly deserialized
    assertEquals(originalAnimals.length, deserializedAnimals.length);

    // Check each animal pair using streams and switch expressions
    java.util.stream.IntStream.range(0, originalAnimals.length).forEach(i -> {
      final var original = originalAnimals[i];
      final var deserialized = deserializedAnimals[i];

      // Check type equality
      assertEquals(original.getClass(), deserialized.getClass());

      // Type-specific checks using switch expression
      switch (original) {
        case Dog origDog -> {
          final var deserDog = (Dog) deserialized;
          assertEquals(origDog.name(), deserDog.name());
          assertEquals(origDog.age(), deserDog.age());
          assertEquals(original, deserialized);
        }
        case Cat origCat -> {
          final var deserCat = (Cat) deserialized;
          assertEquals(origCat.name(), deserCat.name());
          assertEquals(origCat.purrs(), deserCat.purrs());
          assertEquals(original, deserialized);
        }
        case Eagle origEagle -> {
          final var deserEagle = (Eagle) deserialized;
          assertEquals(origEagle.wingspan(), deserEagle.wingspan());
          assertEquals(original, deserialized);
        }
        case Penguin origPenguin -> {
          final var deserPenguin = (Penguin) deserialized;
          assertEquals(origPenguin.canSwim(), deserPenguin.canSwim());
          assertEquals(original, deserialized);
        }
        case Alicorn origAlicorn -> {
          final var deserAlicorn = (Alicorn) deserialized;
          assertEquals(origAlicorn.name(), deserAlicorn.name());
          assertArrayEquals(origAlicorn.magicPowers(), deserAlicorn.magicPowers());
          // Skip equality check for Alicorn due to array field
        }
      }
    });

    // Verify buffer is fully consumed
    assertEquals(0, buffer.remaining(), "Buffer should be fully consumed");
  }

  /// Tests serialization and deserialization of all tree nodes individually
  @Test
  void treeStructurePickling() {
    // Get the standard tree nodes
    final var originalNodes = getTreeNodes();

    // Get a pickler for the TreeNode sealed interface
    final var pickler = picklerForSealedTrait(TreeNode.class);

    // Calculate total buffer size needed - handle each node individually to avoid NPE with null children
    final var totalSize = Arrays.stream(originalNodes)
        .mapToInt(node -> {
            try {
                return pickler.sizeOf(node);
            } catch (NullPointerException e) {
                // Log the problematic node
                System.err.println("NPE calculating size of: " + node);
                throw e;
            }
        })
        .sum();

    // Allocate a single buffer to hold all nodes
    final var buffer = ByteBuffer.allocate(totalSize);

    // Serialize all nodes into the buffer
    Arrays.stream(originalNodes).forEach(node -> pickler.serialize(node, buffer));

    // Prepare buffer for reading
    buffer.flip();

    // Deserialize all nodes from the buffer
    final var deserializedNodes = new TreeNode[originalNodes.length];
    Arrays.setAll(deserializedNodes, ignored -> pickler.deserialize(buffer));

    // Verify all nodes were correctly deserialized
    assertEquals(originalNodes.length, deserializedNodes.length);

    // Check each node pair
    java.util.stream.IntStream.range(0, originalNodes.length).forEach(i -> {
      final var original = originalNodes[i];
      final var deserialized = deserializedNodes[i];

      // Check type equality
      assertEquals(original.getClass(), deserialized.getClass());

      // Type-specific checks using pattern matching
      switch (original) {
        case RootNode origRoot -> {
          final var deserRoot = (RootNode) deserialized;

          // Check structure of left child
          if (origRoot.left() instanceof InternalNode(String name, TreeNode left, TreeNode right)) {
            assertInstanceOf(InternalNode.class, deserRoot.left());
            final var deserLeft = (InternalNode) deserRoot.left();
            assertEquals(name, deserLeft.name());

            // Check leaf nodes under left branch
            if (left instanceof LeafNode(int value)) {
              assertInstanceOf(LeafNode.class, deserLeft.left());
              assertEquals(value, ((LeafNode) deserLeft.left()).value());
            }

            if (right instanceof LeafNode(int value)) {
              assertInstanceOf(LeafNode.class, deserLeft.right());
              assertEquals(value, ((LeafNode) deserLeft.right()).value());
            }
          }

          // Check structure of right child
          if (origRoot.right() instanceof InternalNode origRight) {
            assertInstanceOf(InternalNode.class, deserRoot.right());
            final var deserRight = (InternalNode) deserRoot.right();
            assertEquals(origRight.name(), deserRight.name());

            // Check leaf node under right branch
            if (origRight.left() instanceof LeafNode(int value)) {
              assertInstanceOf(LeafNode.class, deserRight.left());
              assertEquals(value, ((LeafNode) deserRight.left()).value());
            }

            // Check null right child
            assertNull(deserRight.right());
          }
        }
        case InternalNode origInternal -> {
          final var deserInternal = (InternalNode) deserialized;
          assertEquals(origInternal.name(), deserInternal.name());

          // Check children types
          if (origInternal.left() != null) {
            assertNotNull(deserInternal.left());
            assertEquals(origInternal.left().getClass(), deserInternal.left().getClass());
          } else {
            assertNull(deserInternal.left());
          }

          if (origInternal.right() != null) {
            assertNotNull(deserInternal.right());
            assertEquals(origInternal.right().getClass(), deserInternal.right().getClass());
          } else {
            assertNull(deserInternal.right());
          }
        }
        case LeafNode origLeaf -> {
          final var deserLeaf = (LeafNode) deserialized;
          assertEquals(origLeaf.value(), deserLeaf.value());
        }
      }
    });

    // Verify buffer is fully consumed
    assertEquals(0, buffer.remaining(), "Buffer should be fully consumed");
  }
  
  /// Tests serialization and deserialization of the entire tree through just the root node
  @Test
  void treeGraphSerialization() {
    // Get the standard tree nodes
    final var originalNodes = getTreeNodes();
    final var originalRoot = originalNodes[0];
    
    // Get a pickler for the TreeNode sealed interface
    final var pickler = picklerForSealedTrait(TreeNode.class);
    
    // Calculate buffer size needed for just the root node
    final var bufferSize = pickler.sizeOf(originalRoot);
    
    // Allocate a buffer to hold just the root node
    final var buffer = ByteBuffer.allocate(bufferSize);
    
    // Serialize only the root node (which should include the entire graph)
    pickler.serialize(originalRoot, buffer);
    
    // Prepare buffer for reading
    buffer.flip();
    
    // Deserialize the root node (which should reconstruct the entire graph)
    final var deserializedRoot = pickler.deserialize(buffer);
    
    // Validate the entire tree structure was properly deserialized
    // Verify root node structure
    assertInstanceOf(RootNode.class, deserializedRoot);
    final var root = (RootNode) deserializedRoot;

    // Verify left branch (internal1)
    assertInstanceOf(InternalNode.class, root.left());
    final var internal1 = (InternalNode) root.left();
    assertEquals("Branch1", internal1.name());

    // Verify right branch (internal2)
    assertInstanceOf(InternalNode.class, root.right());
    final var internal2 = (InternalNode) root.right();
    assertEquals("Branch2", internal2.name());

    // Verify leaf nodes under internal1
    assertInstanceOf(LeafNode.class, internal1.left());
    final var leaf1 = (LeafNode) internal1.left();
    assertEquals(42, leaf1.value());

    assertInstanceOf(LeafNode.class, internal1.right());
    final var leaf2 = (LeafNode) internal1.right();
    assertEquals(99, leaf2.value());

    // Verify leaf node under internal2
    assertInstanceOf(LeafNode.class, internal2.left());
    final var leaf3 = (LeafNode) internal2.left();
    assertEquals(123, leaf3.value());

    // Verify null right child of internal2
    assertNull(internal2.right());

    // Verify buffer is fully consumed
    assertEquals(0, buffer.remaining(), "Buffer should be fully consumed");
  }

  private static TreeNode[] getTreeNodes() {
    final var leaf1 = new LeafNode(42);
    final var leaf2 = new LeafNode(99);
    final var leaf3 = new LeafNode(123);

    // Internal nodes
    final var internal1 = new InternalNode("Branch1", leaf1, leaf2);
    final var internal2 = new InternalNode("Branch2", leaf3, null);

    // Root node
    final var root = new RootNode(internal1, internal2);

    // Create an array of all tree nodes (excluding null values)
    return new TreeNode[]{root, internal1, internal2, leaf1, leaf2, leaf3};
  }
}
