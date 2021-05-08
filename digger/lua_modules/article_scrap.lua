function main(splash)

    local renderToHTML = splash:jsfunc([[
        function (containerBytes){
            let container = JSON.parse(containerBytes);

            function isSimilarElement(el, query) {
                if (el == undefined) {return false;}
                let isSimilar;
                if (query["tag"] != null && query["tag"].length > 0) {
                  isSimilar = el.nodeName.toLowerCase() == query["tag"];
                }
                if (query["id"] != null && el.id.length > 0) {
                  isSimilar =
                    isSimilar == undefined
                      ? el.id == query["id"]
                      : isSimilar && el.id == query["id"];
                }
                if (el.className != undefined && query["class_list"] != null && query["class_list"].length > 0) {
                  let classExist =
                    query["class_list"].filter((e) => {
                      return el.className.split(" ").indexOf(e) > -1;
                    }).length > 0;
                  isSimilar = isSimilar == undefined ? classExist : isSimilar && classExist;
                }
                if (el.className != undefined && query["exact_class"] != null && query["exact_class"].length > 0) {
                  isSimilar =
                    isSimilar == undefined
                      ? el.className == query["exact_class"]
                      : isSimilar && el.className == query["exact_class"];
                }
              
                return isSimilar == true;
            }

            let cNode;
            let idenNode;
            let disabled = [];

            function traverse() {
                //   console.log(cNode);
                  let desiredNode;
                  if (cNode != undefined && (cNode === idenNode || idenNode.contains(cNode))) {
                        // console.log("Inside identity node or his child",cNode.className, cNode.nodeName);
                        desiredNode = cNode.cloneNode();
                        desiredNode.children = [];
                        Array.prototype.forEach.call(cNode.childNodes,(element) => {

                            if(
                            (container["ignorables"] == null || container["ignorables"].length == 0 || container["ignorables"].filter((ign) => isSimilarElement(element, ign)).length === 0) ||
                                
                            (element.data == undefined || (element.data.indexOf("\n") === -1 || element.data.indexOf("\t") === -1))){
                              
                            cNode = element;
                            let val = traverse();
                            if (val != undefined) {
                                desiredNode.appendChild(val);
                            }
                           }
                        });
                  } else if (cNode.contains(idenNode)) {
                    // console.log("Inside father of identity child", cNode.nodeName, cNode.className);
                    Array.prototype.forEach.call(cNode.childNodes,(element) => {
                      cNode = element;
                      let val = traverse();
                      if (val != undefined) {
                        desiredNode = val;
                      }
                    });
                  } else {
                    // console.log("Some faggot");
                    if (
                      cNode.nodeName != "#text"
                    ) {
                      let str = "";
                      Array.prototype.forEach.call(cNode.classList,(el) => {
                        str += "." + el;
                      });
                      let id = "";
                      if(cNode.id != undefined && cNode.id.length > 0){
                          id = "#"+cNode.id
                      }
                      disabled.push(
                        `${cNode.nodeName.toLowerCase()}${id}${str}`
                      );
                    }
                  }
                  return desiredNode;
            }
            function main(){
              node = ""
              if(container === undefined || container.length == 0 || container['idens'] == undefined || container['idens'].length === 0){
                  node = document.querySelector('body').outerHTML;
              }else{
                  for(let j = 0; j < container["idens"].length; j++){
                    let iden = container['idens'][j];
                    let idenNodes = document.querySelectorAll(iden["param"]);
                    if(idenNodes != null && idenNodes.length > 0){
                        if(iden["is_multiple"] === true && idenNodes.length > 1){
                          node = [];
                          Array.prototype.forEach.call(query, (el)=>{
                            node.push(el.outerHTML)
                          });                       
                        }else{
                          idenNode = idenNodes[0];
                          if(idenNode != undefined && idenNode != null){
                            cNode = document.querySelector('body');
                            node = traverse();
                            if(node != undefined){
                                node = node.outerHTML;
                            }
                            
                        }
                        break;
                      }
                    }
                }
            }
            return JSON.stringify({
                html: node,
                disabled: disabled
            })
          }
        }
    ]])

    assert(splash:go(splash.args.url))
    splash:wait(0.5)
    return {
        url=splash.args.url,
        html=renderToHTML(splash.args.format)
    }
end