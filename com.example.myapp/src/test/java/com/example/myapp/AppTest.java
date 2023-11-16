package com.example.myapp;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import com.example.myapp.App.ExtractWordsFn;

/**
 * Unit test for simple App.
 */
@RunWith(JUnit4.class)
public class AppTest 
{
    @Rule public final transient TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test public void testApp() {
        List<String> words = Arrays.asList(" some input words", "", "\n", "foo");
        Create.of(words);

        PCollection<String> input = p.apply(Create.of(words));
        PCollection<String> output = input.apply(ParDo.of(new ExtractWordsFn()));

        // PCollection は並列処理で順番変わるから、順番変わってOKな Assert を使う
        PAssert.that(output).containsInAnyOrder("some", "input", "words", "foo");

        p.run().waitUntilFinish();        
    }
}
