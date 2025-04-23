// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.simple_pickle;

import io.github.simbo1905.simple_pickle.animal.*;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.github.simbo1905.simple_pickle.Pickler.picklerForSealedTrait;
import static org.junit.jupiter.api.Assertions.*;

class MorePicklerTests {

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
        final var totalSize = Arrays.stream(originalAnimals)
            .mapToInt(pickler::sizeOf)
            .sum();
        
        // Allocate a single buffer to hold all animals
        final var buffer = ByteBuffer.allocate(totalSize);
        
        // Serialize all animals into the buffer using streams
        Arrays.stream(originalAnimals)
            .forEach(animal -> pickler.serialize(animal, buffer));
        
        // Prepare buffer for reading
        buffer.flip();
        
        // Deserialize all animals from the buffer
        final var deserializedAnimals = new Animal[originalAnimals.length];
        Arrays.setAll(deserializedAnimals, i -> pickler.deserialize(buffer));
        
        // Verify all animals were correctly deserialized
        assertEquals(originalAnimals.length, deserializedAnimals.length);
        
        // Check each animal pair using streams and switch expressions
        java.util.stream.IntStream.range(0, originalAnimals.length)
            .forEach(i -> {
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
}
