class EventLogRow extends React.Component {
  render() {
    return (<span>{this.props.text}<br/></span>);
  }
}

class EventLogWindow extends React.Component {
  render() {
    var rows = [];
    var lines = this.props.buffer.split('\n');
    var offset = lines.length - 21;
    if (offset < 0) {
      offset = 0;
    }
    lines.slice(offset).forEach((row, i) => {
      rows.push(<EventLogRow key={i} text={row} />);
    });
    return (
      <pre style={{'height': '20pc', 'border-style': 'solid'}}>
      {rows}
      </pre>
    );
  }
}

class ToggleOnOff extends React.Component {
  constructor(props) {
    super(props);
    this.handleToggleButtonClick = this.handleToggleButtonClick.bind(this);
  }
  
  handleToggleButtonClick() {
    this.props.onToggleButtonClick(this.props.streamEnabled)
  }
  render() {
    return (
      <div>
        <span>Press the button to toggle event start/stop: </span>
        <button onClick={this.handleToggleButtonClick}>{!this.props.streamEnabled ? 'Start' : 'Stop'}</button>
      </div>
    );
  }
}

class SubmitEventBox extends React.Component {
  constructor(props) {
    super(props);
    this.handleTextInputChange= this.handleTextInputChange.bind(this);
  }

  handleTextInputChange(event) {
    this.props.onTextInput(event.target.value)
  }

  render() {
    return (
      <div>
        <span>Enter some event text: </span>
        <input
          type="text"
          placeholder="Event text..."
          value={this.props.eventText}
          onChange={this.handleTextInputChange}
        />
        <button onClick={this.props.submitEvent}>Send</button>
      </div>
    );
  }
}

class StreamingEventWindow extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      logBuffer: '',
      streamEnabled: false,
      streamConnection: {},

      submitEvent: {Text: ''},
      submitLogBuffer: ''
    };
    this.toggleStream = this.toggleStream.bind(this);
    this.handleOpen = this.handleOpen.bind(this);
    this.handleError = this.handleError.bind(this);
    this.handleEvent = this.handleEvent.bind(this);
    this.handleClose = this.handleClose.bind(this);

    this.submitEvent = this.submitEvent.bind(this);
    this.handleEventTextInput = this.handleEventTextInput.bind(this);
  }
  
  toggleStream(streamState) {
    switch (streamState) {
      case false:
        var ws = new WebSocket('ws://' + window.location.hostname + ':' + window.location.port + '/ws');
        ws.onopen = this.handleOpen;
        ws.onerror = this.handleError;
        ws.onmessage = this.handleEvent;
        ws.onclose = this.handleClose;
        this.setState({
          streamEnabled: true,
          streamConnection: ws
        });
        break;
      case true:
        if (!(this.state.streamConnection instanceof WebSocket)) {
          console.error('streamConnection apparently not a websocket instance.')
          break;
        }
        this.state.streamConnection.close(1000, 'normal shutdown')
        this.setState({
          streamEnabled: false,
        });
        break;
    }
  }

  handleOpen(event) {
    this.setState((prevState) => {
      return {logBuffer: prevState.logBuffer + '[INFO] Connection opened.\n'};
    });
  }

  handleError(event) {
    this.setState((prevState) => {
      return {logBuffer: prevState.logBuffer + '[ERROR] Error opening websocket connection\n'};
    });
  }

  handleEvent(event) {
    this.setState((prevState) => {
      return {logBuffer: prevState.logBuffer + '[INFO] Event data:' + event.data.trim() + '\n'};
    });
  }

  handleClose(event) {
    this.setState((prevState) => {
      var message
      if (event.code === 1000) {
        message = 'Normal shutdown';
      } else {
        message = 'Remote connection error';
      }
      return {
        logBuffer: prevState.logBuffer + '[ERROR] Connection closed (' + event.code + '): ' + message + '\n',
        streamEnabled: false
      };
    });
  }

  handleEventTextInput(text) {
    this.setState((prevState) => {
      return {submitEvent: {Text: text}};
    });
  }

  submitEvent() {
    var request = new XMLHttpRequest;
    request.open('POST', window.location.origin + '/publish');
    request.responseType = 'json';
    request.setRequestHeader('Content-Type', 'application/json');
    request.onreadystatechange = function() {
      if (request.readyState === XMLHttpRequest.DONE) {
        var message;
        if (request.status === 200) {
          message = '[INFO] Success - ID: ' + request.response.id + ' Text: ' + this.state.submitEvent.Text + '\n';
        } else {
          message = '[ERROR] Error submitting event (' + request.status + '): ' + request.statusText + '\n';
        }
        this.setState((prevState) => {
          return {
            submitLogBuffer: prevState.submitLogBuffer + message,
            submitEvent: {Text: ''}
          };
        });
      }
    }.bind(this);
    request.send(JSON.stringify(this.state.submitEvent));
  }

  render() {
    return(
      <div>
        <h1>Event Stream Web Demo</h1>
        <div>
          <ToggleOnOff 
            onToggleButtonClick={this.toggleStream}
            streamEnabled={this.state.streamEnabled}
          />
          <EventLogWindow
            buffer={this.state.logBuffer}
          />
        </div>
        <div>
          <SubmitEventBox
            eventText={this.state.submitEvent.Text}
            submitEvent={this.submitEvent}
            onTextInput={this.handleEventTextInput}
          />
          <EventLogWindow
            buffer={this.state.submitLogBuffer}
          />
        </div>
      </div>
    );
  }
}

ReactDOM.render(
  <StreamingEventWindow />,
  document.getElementById('app')
);
