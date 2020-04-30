const React = require('react');
const CompLibrary = require('../../core/CompLibrary');
const Container = CompLibrary.Container;
const MarkdownBlock = CompLibrary.MarkdownBlock;
const cardStyle = require(process.cwd() + "/static/jss/card.js");

class Demos extends React.Component {

    render() {
        const {config: siteConfig} = this.props;
        const Button = props => (
            <div className="pluginWrapper buttonWrapper">
                <a className="button" href={props.href} style={cardStyle.button} target={props.target}>
                    {props.children}
                </a>
            </div>
        );

        const CustomCard = props => {
            return (
                <div style={cardStyle.root}>
                    <div style={cardStyle.title}>
                        <b>
                            <MarkdownBlock>
                                {props.title}
                            </MarkdownBlock>
                        </b>
                    </div>

                    <div style={cardStyle.content}>
                        <div variant="body2" color="textSecondary" component="p">
                            {props.children}
                        </div>
                    </div>
                    <Button href={props.link}>Learn more</Button>
                </div>
            );
        }

        return (
            <div className="docMainWrapper wrapper">
                <Container className="mainContainer documentContainer postContainer" padding={['bottom']}>
                    <header>
                        <h1>{siteConfig.title} Demos</h1>
                    </header>
                    <MarkdownBlock>
                        The following are demonstration applications using Hazelcast Jet. Each is a full application
                        and demonstrates how you can use Jet to solve real-world problems. For smaller, feature
                        specific samples see [Hazelcast Jet Code
                        Samples](https://github.com/hazelcast/hazelcast-jet/tree/master/examples).
                    </MarkdownBlock>
                    <CustomCard title="Flight Telemetry"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/flight-telemetry">
                        This demo analyzes the plane telemetry data stream of all commercial aircraft flying
                        anywhere in the world to compute noise levels and estimated C02 emissions for defined urban
                        areas. The results are displayed in Grafana.
                    </CustomCard>
                    <CustomCard title="Markov Chain Generator"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/markov-chain-generator">
                        This demo generates a Markov Chain with probabilities based on supplied classical
                        books. Markov Chain is a stochastic model describing a sequence of possible events in which
                        the probability of each event depends only on the state attained in the previous event.
                    </CustomCard>
                    <CustomCard title="Real-Time Image Recognition"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/realtime-image-recognition">
                        This demo uses the webcam video stream of a laptop computer as a source
                        and recognizes the objects using machine learning. The image classification is performed
                        using a convolutional neural network pre-trained using a CIFAR-10 dataset.
                    </CustomCard>
                    <CustomCard title="Real-Time Road Traffic Analysis and Prediction"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/road-traffic-predictor">
                        This demo shows how to use Jet for real-time machine learning use-cases. It combines
                        real-time model training and prediction into one Jet Pipeline.
                    </CustomCard>
                    <CustomCard title="TensorFlow"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/tensorflow">
                        TensorFlow is a popular library to train and use machine learning models. We integrate it
                        with Jet to classify stream of events with the result of a TF model execution.
                        This example uses the Large Movie Reviews Dataset as provided by the TensorFlow Keras
                        Datasets. The model predicts whether a movie review is positive or negative.
                    </CustomCard>
                    <CustomCard title="Train Collision Prevention"
                                link="https://github.com/vladoschreiner/transport-tycoon-demo">
                        This demo extracts real-time vehicle data from the train simulation game (Open Transport
                        Tycoon Deluxe) and analyses it using Hazelcast Jet data processing engine. The analytical
                        job in Jet predicts train collisions. The predicted collision information is pushed back to
                        the running OpenTTD game to stop the affected trains.
                    </CustomCard>
                    <CustomCard title="Train Tracking"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/train-track">
                        An example implemented using Apache Beam Jet Runner. It tracks the train using a GPS
                        feed
                        and visualises it on a dashboard. The Beam pipeline is used to enrich the stream of
                        GPS
                        points. They are parsed and then windowing is used to drop some out of sequence
                        points. These points are plotted dynamically on a map to make things more visual
                        using
                        JavaScript and a WebSocket.
                    </CustomCard>
                    <CustomCard title="Twitter Cryptocurrency Sentiment Analysis"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/cryptocurrency-sentiment-analysis">
                        Tweet sentiment is analyzed in real-time to compute cryptocurrency popularity trends.
                        Tweets are streamed from Twitter and categorized by coin type (BTC, ETC, XRP, etc). Then,
                        Natural-language processing(NLP) sentiment analysis is applied to each Tweet to calculate
                        the sentiment score. Jet aggregates scores from the last 30 seconds, last minute and last 5
                        minutes and prints the coin popularity table.
                    </CustomCard>
                    <CustomCard title="Bitcoin Death Cross"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/bitcoin-death-cross">
                        This example shows how Jet is used to spot the dramatically-named Death Cross for the price
                        of Bitcoin, which is an indication to sell, Sell, SELL!. The idea here is that we could
                        automatically analyze stock market prices and use this information to guide our buying and
                        selling decisions.
                    </CustomCard>
                    <CustomCard title="H2O ML Model Inference"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/h2o-breast-cancer-classification">
                        This is an example of enabling H2O data models for use in real-time stream processing by
                        Jet. In this example we use H2O's MOJO model type to execute locally in the Jet runtime. We
                        create an H2O Deep Learning model, train it with a sample data set (Breast Cancer Wisconsin
                        (Diagnostic) Data Set) to prove statistical classification, export the model to a MOJO and
                        incorporate the MOJO into a Jet Pipeline.
                    </CustomCard>
                    <CustomCard title="Change Data Capture Demo With Kafka"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/debezium-cdc-with-kafka">
                        This demo includes an example for Change Data Capture scenario with Debezium, Kafka, MySQL
                        and a Hazelcast Jet cluster inside Docker environment with Docker Compose. By using
                        Debezium, the changes on MySQL table are captured, and then published into a Kafka topic.
                        The Hazelcast Jet pipeline listen for changes on the Kafka topic, logs the events as they
                        arrive to the standard out and puts them to an IMap.
                    </CustomCard>
                    <CustomCard title="Change Data Capture Demo Without Kafka"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/debezium-cdc-without-kafka">
                        This example includes a Jet job that listen for changes on the configured inventory database
                        and logs the events as they arrive to the standard out. Here, The job are going on without
                        using Kafka.
                    </CustomCard>
                </Container>
            </div>
        );
    }
}

Demos.title = "Hazelcast Jet Demos";
module.exports = Demos;
