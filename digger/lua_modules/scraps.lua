
function main(splash)
    
    local renderToHTML =splash:jsfunc(
        [[
        function(container_str){  
        var container = JSON.parse(container_str)  
        function isSimilarElement(el, query) {
            console.log(el, el.className, query);
            let isSimilar;
            if (query["tag"] != null && query["tag"].length > 0) {
              isSimilar = el.tagName.toLowerCase() == query["tag"];
            }
            if (query["id"] != null && el.id.length > 0) {
              isSimilar =
                isSimilar == undefined
                  ? el.id == query["id"]
                  : isSimilar && el.id == query["id"];
            }
            if (query["class_list"] != null && query["class_list"].length > 0) {
              let classExist =
                query["class_list"].filter((e) => {
                  return el.className.split(" ").indexOf(e) > -1;
                }).length > 0;
              isSimilar = isSimilar == undefined ? classExist : isSimilar && classExist;
            }
            if (query["exact_class"] != null && query["exact_class"].length > 0) {
              isSimilar =
                isSimilar == undefined
                  ? el.className == query["exact_class"]
                  : isSimilar && el.className == query["exact_class"];
            }
          
            return isSimilar == true;
          }

          function setStyle(el) {
            let str = "";
            let demo = document.body.appendChild(document.createElement(el.nodeName))
            let styleObj = document.defaultView.getComputedStyle(el, null);
            const defaultStyles = getComputedStyle(demo)
            for (let i = 0; i < styleObj.length; i++) {
              let key = styleObj[i]
              if (defaultStyles.getPropertyValue(key) != styleObj.getPropertyValue(key)){
                str += key+":" + styleObj.getPropertyValue(key) + ";"
              }
            }
            el.setAttribute("style", str);
            demo.remove()
            return el;
          }
          function parse_node(el) {
            let cloneEl = setStyle(el).cloneNode(); // Cloning parent node
            cloneEl.children = []; // Flusing all the childrens
            for (let i = 0; i < el.childNodes.length; i++) {
              let element = el.childNodes[i];
              if(element.data != undefined && (element.data.indexOf("\n") != -1 || element.data.indexOf("\t") != -1)){
                  continue;
              }
              // print("Elements are", element.data)
              console.log("Inside Element Node",container["ignorables"].filter((ign) => isSimilarElement(element, ign)), container["terminations"].filter((ign) => isSimilarElement(element, ign)));
              if (
                container["terminations"] != null &&
                container["terminations"].length > 0 &&
                container["terminations"].filter((ign) => isSimilarElement(element, ign)).length > 0
              ) {
                break;
              } else if (
                container["ignorables"] != null &&
                container["ignorables"].length > 0 &&
                container["ignorables"].filter((ign) => isSimilarElement(element, ign)).length > 0
              ) {
                continue;
              }
              if (element != undefined) {
              //   console.log(element.childNodes);
                if (element.childNodes.length > 0) {
                  cloneEl.appendChild(parse_node(element));
                } else {
                  if (element.nodeType == window.Node.TEXT_NODE) {
                    cloneEl.appendChild(element);
                  } else {
                    cloneEl.appendChild(setStyle(element));
                  }
                }
              }
            }
            return cloneEl;
          }
          function extraction(iden) {
            let parents;
            let is_multiple = container["is_multiple"];
          
            // Checking if identity parameter is different from default is_multiple
            if (iden["is_multiple"] != undefined) {
              is_multiple = iden["is_multiple"];
            }
            parents = document.querySelectorAll(iden["param"]);
            if(is_multiple){

            }
            let div = is_multiple? [] :document.createElement("div");
            if (parents.length > 0) {
              for (let ind = 0; ind < parents.length; ind++) {
                let node = parse_node(parents[ind], iden["param"]);
                if(is_multiple){
                  div.push(node.outerHTML)
                }else{
                  div.appendChild(node)
                }
                if (ind == 0 && is_multiple == false) {
                  break;
                }
              }
            }
            return is_multiple? div : div.outerHTML;
          }

        let nodes = [];
        if(container.length == 0){
          return JSON.stringify(document.body.outerHTML)
        }
        for (let j = 0; j < container["idens"].length; j++) {
            let iden = container["idens"][j];
            nodes.push(extraction(iden));
        }
        return JSON.stringify(nodes)
    }
    ]]
    )
    splash:set_viewport_size(411, 823)
    assert(splash:go(splash.args.url))
    splash:wait(0.5)
    return {
        url = splash.args.url,
        html=renderToHTML(splash.args.format)
    }
end
