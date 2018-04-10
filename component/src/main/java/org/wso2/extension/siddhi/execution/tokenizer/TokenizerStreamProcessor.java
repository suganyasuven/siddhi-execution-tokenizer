package org.wso2.extension.siddhi.execution.tokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * splits a string into words
 */

@Extension(
        name = "getWords",
        namespace = "tokenizer",
        description = "This splits a string into words",
        parameters = {
                @Parameter(name = "text",
                        description = "The input text which should be split.",
                        type = {DataType.STRING})
        },
        examples = @Example(
                syntax = "define stream inputStream (timestamp long, text string);\n" +
                        "@info(name = 'query1')\n" +
                        "from inputStream#tokenizer:getWords(text)\n" +
                        "select timestamp,text\n" +
                        "insert into outputStream;",
                description = "This query performs tokenization for the given string.")
)

public class TokenizerStreamProcessor extends StreamProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenizerStreamProcessor.class);

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        Array<Object> words = new ArrayList<Object>();
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            LOGGER.info("events", streamEvent);
            String event = streamEvent.getOutputData()[0].toString();
            LOGGER.info("String", event);

            StringTokenizer st = new StringTokenizer(event, " :/.,#@*");
            while (st.hasMoreTokens()) {
                words.add(st.nextToken());
                LOGGER.info("Token", st.nextToken());
                List<Object> data =words;
                // If output has values, then add those values to output stream
                complexEventPopulater.populateComplexEvent(streamEvent, data);
            }
        }
        LOGGER.info("wordbucket" + words);
        nextProcessor.process(streamEventChunk);
    }

    /* while (streamEventChunk.hasNext()) {
        StreamEvent event = streamEventChunk.next();
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Event received; Model name: %s Event:%s", modelName, event));
        }

        double[] features = new double[numberOfFeatures];
        for (int i = 0; i < numberOfFeatures; i++) {
            // attributes cannot ever be any other type than double as we've validated the query at init
            features[i] = (double) featureVariableExpressionExecutors.get(i).execute(event);
        }

        Object[] data = PerceptronModelsHolder.getInstance().getPerceptronModel(modelName).classify(features);
        // If output has values, then add those values to output stream
        complexEventPopulater.populateComplexEvent(event, data);
    }
}
        nextProcessor.process(streamEventChunk);*/

    /**
     * The initialization method for {@link StreamProcessor}, which will be called before other methods and validate
     * the all configuration and getting the initial values.
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the Function
     * @param configReader                 this hold the {@link StreamProcessor} extensions configuration reader.
     * @param siddhiAppContext             Siddhi app runtime context
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();

        if (attributeExpressionExecutors.length == 1) {

            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                attributes.add(new Attribute("text", Attribute.Type.STRING));

            } else {
                throw new SiddhiAppCreationException("Parameter should be of type string. But found "
                        + attributeExpressionExecutors[0].getReturnType());
            }
        } else {
            throw new IllegalArgumentException(
                    "Invalid no of arguments passed to tokenizer:getWord() function, "
                            + "required 1, but found " + attributeExpressionExecutors.length);
        }
        return attributes;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {

    }
}
