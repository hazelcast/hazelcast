const React = require('react');
const CompLibrary = require('../../core/CompLibrary');
const Container = CompLibrary.Container;
const MarkdownBlock = CompLibrary.MarkdownBlock;

class Demos extends React.Component {

    render() {
        const {config: siteConfig} = this.props;
        const Button = props => (
            <div className="pluginWrapper buttonWrapper">
                <a className="button" href={props.href} target="_blank" rel="noopener noreferrer">
                    {props.children}
                </a>
            </div>
        );

        const Card = props => {
            return (
                <div className="card">
                    <header>{props.title}</header>
                    <p>{props.children}</p>
                    { props.videoId && 
                      <p>
                        <iframe width="100%" height="300" src={`https://www.youtube.com/embed/${props.videoId}`} 
                        frameBorder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowFullScreen>
                        </iframe>
                      </p>
                    }
                    { props.image && 
                      <img width="100%" src={props.image}></img>
                    }
                    <Button href={props.link}>Learn More</Button>
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
                        The following are demo applications built using Hazelcast Jet. Each demo app 
                        is self-contained and showcase the data processing capabilities of Hazelcast Jet.
                    </MarkdownBlock>
                    <Card title="Real-Time Flight Telemetry"  videoId="WZ5cuZZX0TE"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/flight-telemetry">
                        An application to analyze the real-time telemetry of commercial aircraft currently
                        airbone using data from ADS-B transpoders. It computes noise levels and estimated CO<sub>2</sub> emissions around
                        major airports and urban areas as well as detecting aircraft taking off and landing through 
                        using a simple linear regression model. The results are then written to Graphite and
                        rendered in a dashboard in Grafana.
                    </Card>
                    <Card title="Twitter Cryptocurrency Sentiment Analysis" videoId="GU6VZlfpcIU"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/cryptocurrency-sentiment-analysis">
                        An application to analyze tweet sentiments in real-time to compute cryptocurrency popularity trends.
                        Tweets are streamed from Twitter and categorized by coin type(BTC, ETC, XRP, etc) and
                        natural-language processing(NLP) sentiment analysis is applied to each Tweet to calculate
                        the sentiment score. Jet aggregates scores from the last 30 seconds, last minute and last 5
                        minutes and prints resulting ranking table.
                    </Card>
                    <Card title="Train Collision Prevention" videoId="2RlmCZhhjMY" 
                                link="https://github.com/vladoschreiner/transport-tycoon-demo">
                        Extracts real-time vehicle data from the train simulation game (Open Transport
                        Tycoon Deluxe) and analyses it using Hazelcast Jet. The analytical
                        job predicts train collisions in real-time based on telemetry data supplied from the game.
                        The prediction is pushed back to the running OpenTTD game to stop the affected trains.
                    </Card>
                    <Card title="TensorFlow"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/tensorflow">
                        TensorFlow is a popular library to train and use machine learning models. We integrate it
                        with Jet to classify stream of events with the result of a TF model execution.
                        This example uses the Large Movie Reviews Dataset as provided by the TensorFlow Keras
                        Datasets and builds a model to predict whether a movie review is positive or negative.
                    </Card>
                    <Card title="Real-Time Image Recognition"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/realtime-image-recognition">
                        Uses the webcam video stream of a laptop computer as a source
                        and recognizes the objects using image recognition. The image classification is performed
                        using a convolutional neural network pre-trained using a CIFAR-10 dataset.
                    </Card>
                    <Card title="Markov Chain Generator"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/markov-chain-generator">
                        A Markov Chain generator with probabilities based on supplied classical
                        books. Markov Chain is a stochastic model describing a sequence of possible events in which
                        the probability of each event depends only on the state attained in the previous event.
                    </Card>
                    <Card title="Real-Time Road Traffic Analysis and Prediction"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/road-traffic-predictor">
                        Shows how to use Jet for online machine learning use-cases. It combines
                        real-time model training and prediction into one Jet pipeline to predict traffic
                        patterns.
                    </Card>
                    <Card title="Train Tracking" image="https://github.com/hazelcast/hazelcast-jet-demos/raw/master/train-track/src/site/markdown/images/Screenshot1.png"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/train-track">
                        An application to track and visualize trains in real-time using Jet's Apache Beam Runner. 
                        The application receives a GPS point feed, enriches the stream with static data and 
                        then applies windowing to drop some out of sequence points. The resulting output is 
                        plotted dynamically on a map using JavaScript and WebSockets.
                    </Card>
                    <Card title="Bitcoin Death Cross"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/bitcoin-death-cross">
                        This example shows how Jet is used to spot the dramatically-named Death Cross for the price
                        of Bitcoin, which is an indication to sell, Sell, SELL! The idea here is that we could
                        automatically analyze stock market prices and use this information to guide our buying and
                        selling decisions.
                    </Card>
                    <Card title="H2O ML Model Inference"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/h2o-breast-cancer-classification">
                        This application shows how to integrate a machine learning model generated with H2O with a 
                        Jet pipeline for real-time stream processing. We create an H2O Deep Learning model, train 
                        it with a sample data set (Breast Cancer Wisconsin Diagnostic Data Set) 
                        to prove statistical classification, export the model to a MOJO and 
                        incorporate it into a Jet Pipeline.
                    </Card>
                    <Card title="Change Data Capture (CDC)"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/debezium-cdc-without-kafka">
                        This example includes a Jet job that uses the CDC module of Jet to listen for changes on 
                        the configured inventory database and logs the events as they arrive.
                    </Card>
                    <Card title="Change Data Capture From Kafka"
                                link="https://github.com/hazelcast/hazelcast-jet-demos/tree/master/debezium-cdc-with-kafka">
                        This demo includes an example for Change Data Capture with Debezium, Kafka, MySQL
                        and a Jet cluster inside Docker environment with Docker Compose. Debezium and Kafka Connect is used to 
                        and then published into a Kafka topic.
                        The Hazelcast Jet pipeline listen for changes on the Kafka topic, logs the events as they
                        arrive to the standard out and puts them to an IMap.
                    </Card>
                    <Card title="Enabling Full-text Search with Change Data Capture"
                                link="https://github.com/hazelcast-demos/pet-clinic-index-job">
                        Stream changes using Change Data Capture, enrich the data, correlate (join) the records with
                        other records and finally store the data into an Elasticsearch index, so an application can
                        provide better functionality to the user.
                    </Card>

                </Container>
            </div>
        );
    }
}

Demos.title = "Hazelcast Jet Demos";
module.exports = Demos;
