package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class NestedRecordBenchmark {

    public record Address(String street, String city, String zipCode) implements Serializable {}
    public record Contact(String email, String phone) implements Serializable {}
    public record Employee(String name, int id, Address address, Contact contact) implements Serializable {}
    public record Department(String name, Employee manager, Employee[] members) implements Serializable {}
    
    private Pickler<Department> nfpPickler;
    private Department testData;
    
    // For JDK serialization
    private ByteArrayOutputStream baos;

    @Setup
    public void setup() {
        Address addr1 = new Address("123 Main St", "Anytown", "12345");
        Address addr2 = new Address("456 Oak Ave", "Somewhere", "67890");
        Address addr3 = new Address("789 Pine Rd", "Elsewhere", "11111");
        
        Contact contact1 = new Contact("alice@example.com", "555-0100");
        Contact contact2 = new Contact("bob@example.com", "555-0200");
        Contact contact3 = new Contact("charlie@example.com", "555-0300");
        
        Employee alice = new Employee("Alice", 1001, addr1, contact1);
        Employee bob = new Employee("Bob", 1002, addr2, contact2);
        Employee charlie = new Employee("Charlie", 1003, addr3, contact3);
        
        testData = new Department("Engineering", alice, new Employee[]{bob, charlie});
        
        nfpPickler = Pickler.forClass(Department.class);
        baos = new ByteArrayOutputStream(1024);
    }

    @Benchmark
    public Department nestedNfp() {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        nfpPickler.serialize(buffer, testData);
        buffer.flip();
        return nfpPickler.deserialize(buffer);
    }

    @Benchmark
    public Department nestedJdk() throws Exception {
        baos.reset();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(testData);
        oos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (Department) ois.readObject();
    }
}