package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

public class GenericTypeChainExperiment2 {

  public record DeepDouble(List<List<Optional<Double>>> deepDoubles) {
  }

  public static void main(String[] args) throws Throwable {
    // Analyze the record structure
    RecordReflection<DeepDouble> reflection = RecordReflection.analyze(DeepDouble.class);

    // Log analysis results
    IntStream.range(0, reflection.componentTypes().length)
        .forEach(i -> {
          TypeStructure structure = reflection.componentTypes()[i];
          LOGGER.info(() -> "Component " + i + " type chain: " + structure.tags());
        });

    // Create test data
    DeepDouble testRecord = new DeepDouble(
        List.of(
            List.of(Optional.of(1.23), Optional.empty(), Optional.of(4.56)),
            List.of(Optional.of(7.89))
        )
    );

    LOGGER.info(() -> "\n=== Testing Serialization ===");
    LOGGER.info(() -> "Original: " + testRecord);

    // Serialize
    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer, testRecord);

    LOGGER.info(() -> "Buffer position after serialization: " + writeBuffer.buffer.position());

    // Deserialize
    LOGGER.info(() -> "\n=== Testing Deserialization ===");
    ByteBuffer readBuffer = writeBuffer.flip();
    DeepDouble deserializedRecord = reflection.deserialize(readBuffer);

    LOGGER.info(() -> "Deserialized: " + deserializedRecord);
    LOGGER.info(() -> "Buffer position after deserialization: " + readBuffer.position());

    // Verify equality
    LOGGER.info(() -> "\n=== Verification ===");
    boolean isEqual = deepEquals(testRecord.deepDoubles(), deserializedRecord.deepDoubles());
    LOGGER.info(() -> "Deep equals check: " + isEqual);

    if (!isEqual) {
      LOGGER.warning(() -> "FAIL: Records are not deeply equal!");
    } else {
      LOGGER.info(() -> "SUCCESS: Records are deeply equal.");
    }
  }

  static boolean deepEquals(Object a, Object b) {
    if (a == null || b == null) return a == b;

    if (a instanceof List<?> listA && b instanceof List<?> listB) {
      return listA.size() == listB.size() && IntStream.range(0, listA.size())
          .allMatch(i -> deepEquals(listA.get(i), listB.get(i)));
    }

    if (a instanceof Optional<?> optA && b instanceof Optional<?> optB) {
      return optA.equals(optB);
    }

    return a.equals(b);
  }
}
