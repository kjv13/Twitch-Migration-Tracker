$.ajax({
    url: "http://localhost:5000/streams/viewercount",
    data: {
        string: "hello world",
    },
    success: function(data) {
        console.log(data);
    },
});


var width = 980, height = 500;

// create 200 nodes with radius between 3 and 9
// node[0] is the root node
var nodes = d3.range(12).map(function() { return {radius: Math.random() * 9 + 3}; }),
    // root = nodes[0],
    color = d3.scale.category10();

var root_node = null;
//root.radius = 0;
//root.fixed = true;

var force = d3.layout.force()
      .gravity(0.05)
      // if the node is at 0 (it is root) and its charge is -2000
      // .charge(function(d, i) { return i ? 0 : -2000; })
      .nodes(nodes)
      .size([width, height]);

force.start();

var svg = d3.select("body").append("svg")
      .attr("width", width)
      .attr("height", height);

svg.selectAll("circle")
     //.data(nodes.slice(1))
     .data(nodes)
   .enter().append("circle")
     .attr("r", function(d) { return d.radius; })
     .style("fill", function(d, i) { return color(i % 3); })
     .on("click", function(d, i) {
         window.root_node = i;
     });

force.on("tick", function(e) {
    // a quadtree is a recurssive spatial subdivision
    // each square is split into 4 subsquares
    var q = d3.geom.quadtree(nodes),
        i = 0,
        n = nodes.length;

    // call collide on every node pair
    while (++i < n) q.visit(collide(nodes[i]));

    svg.selectAll("circle")
         .attr("cx", function(d) { return d.x; })
         .attr("cy", function(d) { return d.y; });

    if (window.root_node) {
        var x_distance = (width / 2) - nodes[window.root_node].x;
        if (x_distance > 3) {
            nodes[window.root_node].x += 1;
        }
        else if (x_distance < -3) {
            nodes[window.root_node].x -= 1;
        }
        else {
            nodes[window.root_node].x = width / 2;
        }

        var y_distance = (height / 2) - nodes[window.root_node].y;
        if (y_distance > 3) {
            nodes[window.root_node].y += 1;
        }
        else if (y_distance < -3) {
            nodes[window.root_node].y -= 1;
        }
        else {
            nodes[window.root_node].y = height / 2;
        }
        //nodes[window.root_node].x = width / 2;
        //nodes[window.root_node].y = height / 2;
    }
    force.resume();
});

/*
svg.on("mousemove", function() {
    var p1 = d3.mouse(this);
    root.px = p1[0];
    root.py = p1[1];
    force.resume();
});
*/

function collide(node) {
    var r = node.radius + 16,
        nx1 = node.x - r,
        nx2 = node.x + r,
        ny1 = node.y - r,
        ny2 = node.y + r;
    return function(quad, x1, y1, x2, y2) {
        if (quad.point && (quad.point !== node)) {
            var x = node.x - quad.point.x,
                y = node.y - quad.point.y,
                l = Math.sqrt(x * x + y * y),
                r = node.radius + quad.point.radius;
            if (l < r) {
                l = (l - r) / l * 0.5;
                node.x -= x *= l;
                node.y -= y *= l;
                quad.point.x += x;
                quad.point.y += y;
            }
        }
        return x1 > nx2 || x2 < nx1 || y1 > ny2 || y2 < ny1;
    };
}

