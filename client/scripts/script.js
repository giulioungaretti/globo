const tileUrl = 'https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1IjoibWFwYm94IiwiYSI6ImNpandmbXliNDBjZWd2M2x6bDk3c2ZtOTkifQ._QA7i5Mpkd_m30IGElHziw'

const baseStyle = {
  "color": "#607d8b",
  "weight": 1,
  fillOpacity: 0,
  "opacity":0.5 
};



function getColor(d) {
  return d > 10000 ? '#880E4F' :
    d > 3000 ? '#AD1457' :
      d > 2000 ? '#C2185B' :
        d > 1000 ? '#D81B60' :
          d > 500 ? '#E91E63' :
            d > 200 ? '#EC407A' :
              d > 100 ? '#F06292' :
                '#F48FB1';
}

function style(feature) {
  return {
    color: getColor(feature.properties.count),
    weight: 0,
  };
}

function fetchSimplified(precision, geoJSON, callback) {
  $.ajax({
    url: '/tos2/geojson/multipolygon?precision=' + precision,
    method: 'POST',
    contentType: 'application/json',
    dataType: 'json',
    data: geoJSON,
    success: data => callback(data),
    error: err => callback('Call to /multipolygon failed')
  })
}

function fetchCount(precision, geoJSON, firstDate, secondDate, callback) {
  $.ajax({
    url: '/v1/counts/multipolygon?precision=' + precision + '&start=' + firstDate + '&end=' + secondDate,
    method: 'POST',
    contentType: 'application/json',
    dataType: 'json',
    data: geoJSON,
    success: data => callback(data),
    error: function(xhr) {
      alert(xhr.responseText);
    }
  })
}

class App extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      firstDate: '2015-01-01',
      secondDate: '2015-01-02',
      text: null,
      input: null,
      precision: null,
      result: null,
      info: null
    }
    this.handleInput = this.handleInput.bind(this)
    this.handleSimplify = this.handleSimplify.bind(this)
    this.handleCount = this.handleCount.bind(this)
    this.handleFile = this.handleFile.bind(this)
    this.handleMapData = this.handleMapData.bind(this)
  }

  handleFile(e) {
    var inner = this;
    var reader = new FileReader();
    var file = e.target.files[0];

    reader.onload = function(upload) {
      inner.setState({
        input: upload.target.result,
      });
    }

    reader.readAsText(file);
  }

  handleMapData(data) {
    console.log(data)
    this.setState({
      info: data
    })
  }

  handleInput(name) {
    this.setState({
      [name]: this.refs[name].value
    })
  }

  handleSimplify() {
    const {precision, input} = this.state
    fetchSimplified(precision, input, result => this.setState({
      result
    }))
  }

  handleCount() {
    const {precision, input, firstDate, secondDate} = this.state
    fetchCount(precision, input, firstDate, secondDate, result => this.setState({
      result
    }))
  }

  render() {
    const {firstDate, secondDate, text, input, precision, result, info} = this.state

    return (
      <div id="app">
        <div className="flexbox-container">
            <div className="form">
                  <input ref="firstDate" placeholder="Date from yyyy-mm-dd" className="form__date form__date--first" onChange={this.handleInput.bind(this, 'firstDate')} value={firstDate} />
                  <input ref="secondDate" placeholder="Date to yyyy-mm-dd" className="form__date form__date--second" onChange={this.handleInput.bind(this, 'secondDate')} value={secondDate} />
                  <input ref="text" placeholder="Place Holder" className="form__text" onChange={this.handleInput.bind(this, 'text')} value={text} />
                  <input ref="input" placeholder="paste vaild geoJSON or upload file" className="form__text" onChange={this.handleInput.bind(this, 'input')} value={input} />
                  <input ref="precision" placeholder="precision (1-30)" className="form__precision" onChange={this.handleInput.bind(this, 'precision')} value={precision} />
                  <input ref="the-file-input" type="file"  onChange={this.handleFile} />
                  <button className="form__submit" onClick={this.handleSimplify}>Simplify</button>
                  <button className="form__submit" onClick={this.handleCount}>Count</button>
              </div>
                  {result ?
                    <div className="map-container">  <Map data={result} input={JSON.parse(input)} sendDataToParent={this.handleMapData} /></div> : <div className="hero"><span > GLOBO </span> </div>
      }
                  {info ?
        <Info data={info} /> : null
      }
        </div>
     </div>
    )
  }
}

class Info extends React.Component {
  constructor(props) {
    super(props)
  }

  render() {
    console.log(this)
    return (
      <div className="info">
        <h1> Details </h1>
        <p> Hover over a region: </p>
           Counts:{this.props.data}
          </div>
    )
  }
}

var Map = React.createClass({
  //propTypes : {
    //myFunc: React.PropTypes.func,
  //},
  componentDidMount: function() {
    var map = this.map = L.map(ReactDOM.findDOMNode(this), {
      minZoom: 2,
      maxZoom: 20,
      layers: [
        L.tileLayer(tileUrl, {
          maxZoom: 15,
          attribution: 'mapbox',
          id: 'mapbox.light'
        })
      ],
      attributionControl: false,
    });
    map.on('click', this.onMapClick);
    this.base = this.addGeoJsonLayer(this.map, this.props.input, baseStyle)
    this.layer = this.addGeoJsonLayer(this.map, this.props.data, style)
  },
  componentWillUnmount: function() {
    this.map.off('click', this.onMapClick);
    this.map = null;
  },
  onMapClick: function() {
    console.log(this)
    console.log(this.props.data);
  },
  addGeoJsonLayer: function(map, data, style) {
    var self = this
    if (data !== null && map) {
      // no map or data -> no layer
      var layer = L.geoJson(undefined, {
        onEachFeature: function(feature, layer) {
          layer.on({
            // TODO this shoould do some magic with react
            // update parent state
            mouseover: self.logvalue,
          })
        }
      })
      layer.addTo(map)
      layer.addData(data)
      layer.setStyle(style)
      map.fitBounds(layer.getBounds())
      return layer
    }
  },
  logvalue: function(e) {
    var layer = e.target
    this.props.sendDataToParent(layer.feature.properties.count)
  },
  shouldComponentUpdate: function(nextProps, nextState) {
    return nextProps.data !== this.props.data;
  },
  componentWillUpdate: function(nextProps, nextState) {
    // perform any preparations for an upcoming update
    // remove old layers because we have new state
    this.map.removeLayer(this.layer)
    this.map.removeLayer(this.base)
  },
  render: function() {
    console.log(this)
    this.base = this.addGeoJsonLayer(this.map, this.props.input, baseStyle)
    this.layer = this.addGeoJsonLayer(this.map, this.props.data, style)
    return (
      <div className = 'map' >
      </div>
      );
  }
});
ReactDOM.render(< App / >, document.getElementById('content'))
