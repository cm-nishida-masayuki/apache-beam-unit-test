package com.example.myapp;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Hello world!
 *
 */
public class App 
{
    public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

    static class ExtractWordsFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            String[] words = element.split(TOKENIZER_PATTERN, -1);

            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    public static void main( String[] args )
    {
        Pipeline p = Pipeline.create();
        List<String> words = Arrays.asList("aaa", "bbb ccc", "ccc", "", "c", "\n", "aa");

        p.apply(Create.of(words))
//            .apply(ParDo.of(new ExtractWordsFn()))
            .apply(ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext context) {
                    System.out.println(context.element());
                    context.output(context.element());
                }
            }));

        p.run().waitUntilFinish();        
    }
}
