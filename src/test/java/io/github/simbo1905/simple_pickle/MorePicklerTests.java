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

    @Test
    void allAnimalsInBuffer() {
        // Create instances of all animal types
        Dog dog = new Dog("Buddy", 3);
        Cat cat = new Cat("Whiskers", true);
        Eagle eagle = new Eagle(2.1);
        Penguin penguin = new Penguin(true);
        Alicorn alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});
        
        // Create an array of all animals
        Animal[] originalAnimals = {dog, cat, eagle, penguin, alicorn};
        
        // Get a pickler for the Animal sealed interface
        Pickler<Animal> pickler = picklerForSealedTrait(Animal.class);
        
        // Calculate total buffer size needed
        int totalSize = 0;
        for (Animal animal : originalAnimals) {
            totalSize += pickler.sizeOf(animal);
        }
        
        // Allocate a single buffer to hold all animals
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Serialize all animals into the buffer
        for (Animal animal : originalAnimals) {
            pickler.serialize(animal, buffer);
        }
        
        // Prepare buffer for reading
        buffer.flip();
        
        // Deserialize all animals from the buffer
        Animal[] deserializedAnimals = new Animal[originalAnimals.length];
        for (int i = 0; i < originalAnimals.length; i++) {
            deserializedAnimals[i] = pickler.deserialize(buffer);
        }
        
        // Verify all animals were correctly deserialized
        assertEquals(originalAnimals.length, deserializedAnimals.length);
        
        // Check each animal individually
        for (int i = 0; i < originalAnimals.length; i++) {
            Animal original = originalAnimals[i];
            Animal deserialized = deserializedAnimals[i];
            
            // Check type and equality
            assertEquals(original.getClass(), deserialized.getClass());
            
            // Type-specific checks
            if (original instanceof Dog) {
                Dog origDog = (Dog) original;
                Dog deserDog = (Dog) deserialized;
                assertEquals(origDog.name(), deserDog.name());
                assertEquals(origDog.age(), deserDog.age());
            } else if (original instanceof Cat) {
                Cat origCat = (Cat) original;
                Cat deserCat = (Cat) deserialized;
                assertEquals(origCat.name(), deserCat.name());
                assertEquals(origCat.purrs(), deserCat.purrs());
            } else if (original instanceof Eagle) {
                Eagle origEagle = (Eagle) original;
                Eagle deserEagle = (Eagle) deserialized;
                assertEquals(origEagle.wingspan(), deserEagle.wingspan());
            } else if (original instanceof Penguin) {
                Penguin origPenguin = (Penguin) original;
                Penguin deserPenguin = (Penguin) deserialized;
                assertEquals(origPenguin.canSwim(), deserPenguin.canSwim());
            } else if (original instanceof Alicorn) {
                Alicorn origAlicorn = (Alicorn) original;
                Alicorn deserAlicorn = (Alicorn) deserialized;
                assertEquals(origAlicorn.name(), deserAlicorn.name());
                assertArrayEquals(origAlicorn.magicPowers(), deserAlicorn.magicPowers());
            }
            
            // General equality check - skip Alicorn since we've already checked its fields individually
            // (arrays don't properly implement equals, so the default Record equals won't work correctly)
            if (!(original instanceof Alicorn)) {
                assertEquals(original, deserialized);
            }
        }
        
        // Verify buffer is fully consumed
        assertEquals(buffer.remaining(), 0, "Buffer should be fully consumed");
    }
}
