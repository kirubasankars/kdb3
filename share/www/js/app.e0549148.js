(function(t){function e(e){for(var r,c,i=e[0],s=e[1],u=e[2],f=0,d=[];f<i.length;f++)c=i[f],Object.prototype.hasOwnProperty.call(a,c)&&a[c]&&d.push(a[c][0]),a[c]=0;for(r in s)Object.prototype.hasOwnProperty.call(s,r)&&(t[r]=s[r]);l&&l(e);while(d.length)d.shift()();return o.push.apply(o,u||[]),n()}function n(){for(var t,e=0;e<o.length;e++){for(var n=o[e],r=!0,i=1;i<n.length;i++){var s=n[i];0!==a[s]&&(r=!1)}r&&(o.splice(e--,1),t=c(c.s=n[0]))}return t}var r={},a={app:0},o=[];function c(e){if(r[e])return r[e].exports;var n=r[e]={i:e,l:!1,exports:{}};return t[e].call(n.exports,n,n.exports,c),n.l=!0,n.exports}c.m=t,c.c=r,c.d=function(t,e,n){c.o(t,e)||Object.defineProperty(t,e,{enumerable:!0,get:n})},c.r=function(t){"undefined"!==typeof Symbol&&Symbol.toStringTag&&Object.defineProperty(t,Symbol.toStringTag,{value:"Module"}),Object.defineProperty(t,"__esModule",{value:!0})},c.t=function(t,e){if(1&e&&(t=c(t)),8&e)return t;if(4&e&&"object"===typeof t&&t&&t.__esModule)return t;var n=Object.create(null);if(c.r(n),Object.defineProperty(n,"default",{enumerable:!0,value:t}),2&e&&"string"!=typeof t)for(var r in t)c.d(n,r,function(e){return t[e]}.bind(null,r));return n},c.n=function(t){var e=t&&t.__esModule?function(){return t["default"]}:function(){return t};return c.d(e,"a",e),e},c.o=function(t,e){return Object.prototype.hasOwnProperty.call(t,e)},c.p="/_utils/";var i=window["webpackJsonp"]=window["webpackJsonp"]||[],s=i.push.bind(i);i.push=e,i=i.slice();for(var u=0;u<i.length;u++)e(i[u]);var l=s;o.push([0,"chunk-vendors"]),n()})({0:function(t,e,n){t.exports=n("56d7")},"034f":function(t,e,n){"use strict";var r=n("64a9"),a=n.n(r);a.a},"56d7":function(t,e,n){"use strict";n.r(e);n("cadf"),n("551c"),n("f751"),n("097d");var r=n("2b0e"),a=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{class:{closedNav:t.closedNav},attrs:{id:"app"}},[n("NavBar",{on:{toggleNav:t.toggleNav}}),n("div",{staticClass:"dashboard"},[n("div",{staticClass:"toppanel"},[n("router-view",{attrs:{name:"topBar"}})],1),n("div",{staticClass:"contentpanel"},[n("router-view")],1)])],1)},o=[],c=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{attrs:{id:"navbar"}},[n("ul",{staticClass:"top"},[n("li",{staticClass:"item"},[n("router-link",{attrs:{to:"/"}},[t._v("Databases")])],1),n("li",{staticClass:"item"},[n("router-link",{attrs:{to:"/verify"}},[t._v("Verify")])],1)]),n("ul",{staticClass:"bottom"},[n("li",{staticClass:"item",on:{click:t.toggleNav}},[t._v("Close")])])])},i=[],s={methods:{toggleNav:function(){this.$emit("toggleNav")}}},u=s,l=(n("8a29"),n("2877")),f=Object(l["a"])(u,c,i,!1,null,"631106b9",null),d=f.exports,p={name:"app",components:{NavBar:d},data:function(){return{closedNav:!1}},methods:{toggleNav:function(){this.closedNav=!this.closedNav}}},v=p,b=(n("034f"),Object(l["a"])(v,a,o,!1,null,null,null)),h=b.exports,m=n("8c4f"),_=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{},[n("ul",{staticStyle:{"list-style":"none"},attrs:{id:"example-1"}},t._l(t.listDatabases,(function(e){return n("li",{key:e.name},[n("router-link",{attrs:{to:{name:"all_docs",params:{db:e.db_name}}}},[t._v(t._s(e.db_name))])],1)})),0)])},O=[],g=(n("8e6e"),n("ac6a"),n("456d"),n("bd86")),y=n("2f62");function j(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function w(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?j(n,!0).forEach((function(e){Object(g["a"])(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):j(n).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}var D={name:"databases",computed:w({},Object(y["b"])(["listDatabases"])),created:function(){this.$store.dispatch("fetchDatabases")}},E=D,P=Object(l["a"])(E,_,O,!1,null,null,null),S=P.exports,$=function(){var t=this,e=t.$createElement;t._self._c;return t._m(0)},N=[function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{staticClass:"about"},[n("h1",[t._v("This is an verify")])])}],T={},x=Object(l["a"])(T,$,N,!1,null,null,null),k=x.exports,C=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{staticStyle:{width:"100%",height:"100%"}},[n("table",{staticStyle:{width:"100%"}},[t._m(0),t._l(t.listViewItems,(function(e){return n("tr",{key:e.id,on:{click:function(n){return t.navigateDoc(e)}}},[n("td",[t._v(t._s(e.key))]),n("td",[t._v(t._s(e.value))]),n("td",[t._v(t._s(e.id))])])})),n("tr")],2)])},A=[function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("thead",[n("tr",[n("td",[t._v("Key")]),n("td",[t._v("Value")]),n("td",[t._v("Id")])])])}];function V(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function B(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?V(n,!0).forEach((function(e){Object(g["a"])(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):V(n).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}var I={name:"DatabaseView",computed:B({},Object(y["b"])(["listViewItems"])),methods:{navigateDoc:function(t){this.$router.push({name:"document",params:{db:this.$route.params.db,doc_id:t.id}})}},created:function(){this.$store.dispatch("fetchView",this.$route.params.db,"all_docs")}},M=I,J=Object(l["a"])(M,C,A,!1,null,null,null),U=J.exports,F=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("Editor",{attrs:{content:t.getDocument}})},W=[],K=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{staticStyle:{width:"100%",height:"80%"}},[n("textarea",{directives:[{name:"model",rawName:"v-model",value:t.value,expression:"value"}],staticStyle:{width:"100%",height:"100%"},attrs:{id:"docEditor"},domProps:{value:t.value},on:{input:function(e){e.target.composing||(t.value=e.target.value)}}}),n("button",{on:{click:t.save}},[t._v("Save")])])},q=[],z={props:["content"],data:function(){return{value:{}}},watch:{content:function(t){this.value=JSON.stringify(t,null,2)}},methods:{save:function(){this.$store.dispatch("saveDocument",{db:this.$route.params.db,doc:JSON.parse(this.value)})}}},G=z,H=Object(l["a"])(G,K,q,!1,null,null,null),L=H.exports;function Q(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function R(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?Q(n,!0).forEach((function(e){Object(g["a"])(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):Q(n).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}var X={components:{Editor:L},computed:R({},Object(y["b"])(["getDocument"])),created:function(){this.$store.dispatch("fetchDocument",this.$route.params)}},Y=X,Z=Object(l["a"])(Y,F,W,!1,null,null,null),tt=Z.exports,et=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",[t._v(t._s(this.$route.params.db))])},nt=[],rt={},at=Object(l["a"])(rt,et,nt,!1,null,null,null),ot=at.exports;r["a"].use(m["a"]);var ct=new m["a"]({base:"/_utils/",routes:[{path:"/",name:"databases",components:{default:S,topBar:ot}},{path:"/database/:db/_all_docs",name:"all_docs",components:{default:U,topBar:ot}},{path:"/database/:db/:doc_id",name:"document",components:{default:tt,topBar:ot}},{path:"/verify",name:"verifyinstall",components:{default:k,topBar:ot}}]}),it=n("bc3a"),st=n.n(it);r["a"].use(y["a"]);var ut="http://localhost:8001",lt=new y["a"].Store({state:{dbs_info:[],view:[],document:{}},mutations:{SET_DATABASES_INFO:function(t,e){t.dbs_info=e},SET_VIEW_DATA:function(t,e){t.view=e},SET_DOCUMENT:function(t,e){t.document=e}},actions:{fetchDatabases:function(t){st.a.get(ut+"/_all_dbs").then((function(e){var n=[];for(var r in e.data)n.push(st.a.get(ut+"/"+e.data[r]));st.a.all(n).then((function(e){var n=[];for(var r in e)n.push(e[r].data);t.commit("SET_DATABASES_INFO",n)}))}))},fetchView:function(t,e){st.a.get(ut+"/"+e+"/_all_docs").then((function(e){t.commit("SET_VIEW_DATA",e.data.rows)}))},fetchDocument:function(t,e){console.log(ut+"/"+e.db+"/"+e.doc_id),st.a.get(ut+"/"+e.db+"/"+e.doc_id).then((function(e){t.commit("SET_DOCUMENT",e.data),console.log("fetched doc",e.data)}))},saveDocument:function(t,e){console.log(e),st.a.post(ut+"/"+e.db,e.doc).then((function(){st.a.get(ut+"/"+e.db+"/"+e.doc._id).then((function(e){t.commit("SET_DOCUMENT",e.data)}))}))}},getters:{listDatabases:function(t){return t.dbs_info},listViewItems:function(t){return t.view},getDocument:function(t){return t.document}}});r["a"].config.productionTip=!1,new r["a"]({router:ct,store:lt,render:function(t){return t(h)}}).$mount("#app")},"64a9":function(t,e,n){},"8a29":function(t,e,n){"use strict";var r=n("a4ae"),a=n.n(r);a.a},a4ae:function(t,e,n){}});
//# sourceMappingURL=app.e0549148.js.map