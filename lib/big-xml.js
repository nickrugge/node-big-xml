const expat = require('node-expat'),
    fs = require('fs'),
    events = require('events'),
    util = require('util'),
    stream = require('stream'),
    zlib = require('zlib');
    
exports.createReader = function(file, recordRegEx, options) {
  return new BigXmlReader(file, recordRegEx, options);
};

function isReadableStream(obj) {
  return obj instanceof stream.Stream &&
    typeof (obj._read === 'function') &&
    typeof (obj._readableState === 'object');
}
    
function BigXmlReader(file, recordRegEx, options) {
  const self = this;
  
  options = options || {};
  options.gzip = options.gzip || false;
  
  const parser = new expat.Parser('UTF-8');

  let stream;
  if(isReadableStream(file)) stream = file;
  else stream = fs.createReadStream(file);
  
  if (options.gzip) {
    const gunzip = zlib.createGunzip();
    stream.pipe(gunzip);
    stream = gunzip;
  }

  stream.on('data', function(rawData) {
    const sanitizedData = rawData
      .toString()
      .replace(/&(?:\#(?:(?<dec>[0-9]+)|[Xx](?<hex>[0-9A-Fa-f]+))|(?<named>[A-Za-z0-9]+));/g, ' ');
    const data = Buffer.from(sanitizedData);
    if (!parser.parse(data)) {
      self.emit('error', new Error('XML Error: ' + parser.getError()));
    } else {
      self.emit('data', data);
    }
  });

  stream.on('error', function(err) {
    self.emit('error', new Error(err));
  });
  
  ///////////////////////////

  let node = {};
  let nodes = [];
  let record;
  let isCapturing = false;
  let level = 0;
  
  parser.on('startElement', function(name, attrs) {
    level++;
    
    if (!isCapturing && !name.match(recordRegEx)) {
      return;
    }
    else if (!isCapturing) {
      isCapturing = true;
      node = {};
      nodes = [];
      record = undefined;
    }
    
    if (node.children === undefined) {
      node.children = [];
    }
   
    const child = { tag: name };
    node.children.push(child);
    
    if (Object.keys(attrs).length > 0) {
      child.attrs = attrs;
    }
    
    nodes.push(node);
    node = child;

    if (name.match(recordRegEx)) {
      record = node;
    }
  });

  parser.on('text', function(txt) {
    if (!isCapturing) {
      return;
    }
    
    if (txt.length > 0) {
      if (node.text === undefined) {
        node.text = txt;
      } else {
        node.text += txt;
      }
    }
  });

  parser.on('endElement', function(name) {
    level--;
    node = nodes.pop();
    
    if (name.match(recordRegEx)) {
      isCapturing = false;
      self.emit('record', record);
    }
    
    if (level === 0) {
      self.emit('end');
    }
    
  });

  self.pause = function() {
    stream.pause();
  };

  self.resume = function() {
    stream.resume();
  };
}
util.inherits(BigXmlReader, events.EventEmitter);
