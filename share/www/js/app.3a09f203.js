(function(t){function e(e){for(var r,c,i=e[0],s=e[1],u=e[2],d=0,f=[];d<i.length;d++)c=i[d],Object.prototype.hasOwnProperty.call(a,c)&&a[c]&&f.push(a[c][0]),a[c]=0;for(r in s)Object.prototype.hasOwnProperty.call(s,r)&&(t[r]=s[r]);l&&l(e);while(f.length)f.shift()();return o.push.apply(o,u||[]),n()}function n(){for(var t,e=0;e<o.length;e++){for(var n=o[e],r=!0,i=1;i<n.length;i++){var s=n[i];0!==a[s]&&(r=!1)}r&&(o.splice(e--,1),t=c(c.s=n[0]))}return t}var r={},a={app:0},o=[];function c(e){if(r[e])return r[e].exports;var n=r[e]={i:e,l:!1,exports:{}};return t[e].call(n.exports,n,n.exports,c),n.l=!0,n.exports}c.m=t,c.c=r,c.d=function(t,e,n){c.o(t,e)||Object.defineProperty(t,e,{enumerable:!0,get:n})},c.r=function(t){"undefined"!==typeof Symbol&&Symbol.toStringTag&&Object.defineProperty(t,Symbol.toStringTag,{value:"Module"}),Object.defineProperty(t,"__esModule",{value:!0})},c.t=function(t,e){if(1&e&&(t=c(t)),8&e)return t;if(4&e&&"object"===typeof t&&t&&t.__esModule)return t;var n=Object.create(null);if(c.r(n),Object.defineProperty(n,"default",{enumerable:!0,value:t}),2&e&&"string"!=typeof t)for(var r in t)c.d(n,r,function(e){return t[e]}.bind(null,r));return n},c.n=function(t){var e=t&&t.__esModule?function(){return t["default"]}:function(){return t};return c.d(e,"a",e),e},c.o=function(t,e){return Object.prototype.hasOwnProperty.call(t,e)},c.p="/_utils/";var i=window["webpackJsonp"]=window["webpackJsonp"]||[],s=i.push.bind(i);i.push=e,i=i.slice();for(var u=0;u<i.length;u++)e(i[u]);var l=s;o.push([0,"chunk-vendors"]),n()})({0:function(t,e,n){t.exports=n("56d7")},"034f":function(t,e,n){"use strict";var r=n("64a9"),a=n.n(r);a.a},"56d7":function(t,e,n){"use strict";n.r(e);n("cadf"),n("551c"),n("f751"),n("097d");var r=n("2b0e"),a=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{class:{closedNav:t.closedNav},attrs:{id:"app"}},[n("NavBar",{on:{toggleNav:t.toggleNav}}),n("div",{staticClass:"dashboard"},[n("div",{staticClass:"toppanel"},[n("router-view",{attrs:{name:"topBar"}})],1),n("div",{staticClass:"contentpanel",staticStyle:{overflow:"scroll"}},[n("router-view")],1)])],1)},o=[],c=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{attrs:{id:"navbar"}},[n("ul",{staticClass:"top"},[n("li",{staticClass:"item"},[n("router-link",{attrs:{to:"/"}},[t._v("Databases")])],1),n("li",{staticClass:"item"},[n("router-link",{attrs:{to:"/verify"}},[t._v("Verify")])],1)]),n("ul",{staticClass:"bottom"},[n("li",{staticClass:"item",on:{click:t.toggleNav}},[t._v("Close")])])])},i=[],s={methods:{toggleNav:function(){this.$emit("toggleNav")}}},u=s,l=(n("8a29"),n("2877")),d=Object(l["a"])(u,c,i,!1,null,"631106b9",null),f=d.exports,p={name:"app",components:{NavBar:f},data:function(){return{closedNav:!1}},methods:{toggleNav:function(){this.closedNav=!this.closedNav}}},b=p,m=(n("034f"),Object(l["a"])(b,a,o,!1,null,null,null)),h=m.exports,v=n("8c4f"),O=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{},[n("ul",{staticStyle:{"list-style":"none"},attrs:{id:"example-1"}},t._l(t.listDatabases,(function(e){return n("li",{key:e.name},[n("router-link",{attrs:{to:{name:"all_docs",params:{db:e.db_name}}}},[t._v(t._s(e.db_name))])],1)})),0)])},_=[],y=(n("8e6e"),n("ac6a"),n("456d"),n("bd86")),g=n("2f62");function w(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function j(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?w(n,!0).forEach((function(e){Object(y["a"])(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):w(n).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}var D={name:"databases",computed:j({},Object(g["b"])(["listDatabases"])),created:function(){this.$store.dispatch("fetchDatabases")}},E=D,P=Object(l["a"])(E,O,_,!1,null,null,null),S=P.exports,$=function(){var t=this,e=t.$createElement;t._self._c;return t._m(0)},T=[function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{staticClass:"about"},[n("h1",[t._v("This is an verify")])])}],k={},A=Object(l["a"])(k,$,T,!1,null,null,null),N=A.exports,C=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{staticStyle:{width:"100%",height:"100%",overflow:"scroll"}},[n("table",{staticStyle:{width:"100%"}},[t._m(0),t._l(t.listViewItems,(function(e){return n("tr",{key:e.id,on:{click:function(n){return t.navigateDoc(e)}}},[n("td",[t._v(t._s(e.key))]),n("td",[t._v(t._s(e.value))]),n("td",[t._v(t._s(e.id))])])})),n("tr")],2)])},x=[function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("thead",[n("tr",[n("td",[t._v("Key")]),n("td",[t._v("Value")]),n("td",[t._v("Id")])])])}];function B(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function V(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?B(n,!0).forEach((function(e){Object(y["a"])(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):B(n).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}var M={name:"DatabaseView",computed:V({},Object(g["b"])(["listViewItems"])),methods:{navigateDoc:function(t){this.$router.push({name:"document",params:{db:this.$route.params.db,doc_id:t.id}})}},created:function(){this.$store.dispatch("fetchView",this.$route.params.db,"all_docs")}},I=M,J=Object(l["a"])(I,C,x,!1,null,null,null),U=J.exports,F=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("Editor",{attrs:{content:t.getDocument}})},W=[],z=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{staticClass:"editor"},[n("div",[n("button",{on:{click:t.save}},[t._v("Save")])]),n("div",{attrs:{id:"docEditor"}})])},K=[],X={props:["content"],data:function(){return{editor:null,beforeContent:""}},watch:{content:function(t){var e=this;this.editor.setValue(JSON.stringify(t,null,2)),null==this.beforeContent&&setTimeout((function(){e.$router.push({name:"all_docs",params:{db:e.$route.params.db}})}),500),this.beforeContent=t}},mounted:function(){this.editor=window.ace.edit("docEditor"),this.editor.setTheme("ace/theme/dracula"),this.editor.session.setMode("ace/mode/json"),this.editor.setFontSize(14),this.editor.setOption("showPrintMargin",!1),this.editor.setValue("{\n\t\n}")},methods:{save:function(){this.$store.dispatch("saveDocument",{db:this.$route.params.db,doc:JSON.parse(this.editor.getValue())}),this.beforeContent=null}}},q=X,G=(n("aea8"),Object(l["a"])(q,z,K,!1,null,"153d32f8",null)),H=G.exports;function L(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function Q(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?L(n,!0).forEach((function(e){Object(y["a"])(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):L(n).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}var R={components:{Editor:H},computed:Q({},Object(g["b"])(["getDocument"])),created:function(){this.$store.dispatch("fetchDocument",this.$route.params)}},Y=R,Z=Object(l["a"])(Y,F,W,!1,null,null,null),tt=Z.exports,et=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",[n("input",{directives:[{name:"model",rawName:"v-model",value:t.name,expression:"name"}],attrs:{placeholder:"edit me"},domProps:{value:t.name},on:{input:function(e){e.target.composing||(t.name=e.target.value)}}}),n("button",{on:{click:t.create}},[t._v("Create")])])},nt=[];n("7f7f");function rt(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function at(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?rt(n,!0).forEach((function(e){Object(y["a"])(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):rt(n).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}var ot={data:function(){return{name:""}},computed:at({},Object(g["b"])(["getDatabase"])),watch:{getDatabase:function(t){this.$router.push({name:"all_docs",params:{db:t}})}},mounted:function(){this.name=""},methods:{create:function(){this.$store.dispatch("createDatabase",this.name)}}},ct=ot,it=Object(l["a"])(ct,et,nt,!1,null,null,null),st=it.exports,ut=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",[n("router-link",{attrs:{to:{name:"newdatabase"}}},[t._v("Create Database")])],1)},lt=[],dt={},ft=Object(l["a"])(dt,ut,lt,!1,null,null,null),pt=ft.exports,bt=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",[n("span",[t._v(t._s(this.$route.params.db))]),n("button",{on:{click:t.deleteDatabase}},[t._v("X")]),n("button",{on:{click:t.newDocument}},[t._v("+")])])},mt=[];function ht(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function vt(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?ht(n,!0).forEach((function(e){Object(y["a"])(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):ht(n).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}var Ot={computed:vt({},Object(g["b"])(["getDatabase"])),watch:{getDatabase:function(t){""==t&&this.$router.push({name:"databases",params:{}})}},mounted:function(){this.$store.dispatch("setDatabase",this.$route.params.db)},methods:{deleteDatabase:function(){this.$store.dispatch("deleteDatabase",this.$route.params.db)},newDocument:function(){this.$store.dispatch("newDocument"),this.$router.push({name:"document",params:{db:this.$route.params.db,doc_id:"_new"}})}}},_t=Ot,yt=Object(l["a"])(_t,bt,mt,!1,null,null,null),gt=yt.exports;r["a"].use(v["a"]);var wt=new v["a"]({base:"/_utils/",routes:[{path:"/",name:"databases",components:{default:S,topBar:pt}},{path:"/newdatabase",name:"newdatabase",components:{default:st,topBar:pt}},{path:"/database/:db/_all_docs",name:"all_docs",components:{default:U,topBar:gt}},{path:"/database/:db/:doc_id",name:"document",components:{default:tt,topBar:gt}},{path:"/verify",name:"verifyinstall",components:{default:N,topBar:pt}}]}),jt=n("bc3a"),Dt=n.n(jt);r["a"].use(g["a"]);var Et=window.location.origin+"",Pt=new g["a"].Store({state:{dbs_info:[],view:[],document:{},database:""},mutations:{SET_DATABASES_INFO:function(t,e){t.dbs_info=e},SET_VIEW_DATA:function(t,e){t.view=e},SET_DOCUMENT:function(t,e){t.document=e},SET_DATABASE:function(t,e){t.database=e}},actions:{fetchDatabases:function(t){Dt.a.get(Et+"/_all_dbs").then((function(e){var n=[];for(var r in e.data)n.push(Dt.a.get(Et+"/"+e.data[r]));Dt.a.all(n).then((function(e){var n=[];for(var r in e)n.push(e[r].data);t.commit("SET_DATABASES_INFO",n)}))}))},fetchView:function(t,e){Dt.a.get(Et+"/"+e+"/_all_docs").then((function(e){t.commit("SET_VIEW_DATA",e.data.rows)}))},fetchDocument:function(t,e){Dt.a.get(Et+"/"+e.db+"/"+e.doc_id).then((function(e){t.commit("SET_DOCUMENT",e.data)}))},saveDocument:function(t,e){Dt.a.post(Et+"/"+e.db,e.doc).then((function(n){Dt.a.get(Et+"/"+e.db+"/"+n.data._id).then((function(e){t.commit("SET_DOCUMENT",e.data)}))}))},createDatabase:function(t,e){Dt.a.put(Et+"/"+e).then((function(n){n.data.ok&&t.commit("SET_DATABASE",e)}))},deleteDatabase:function(t,e){Dt.a.delete(Et+"/"+e).then((function(e){e.data.ok&&t.commit("SET_DATABASE","")}))},setDatabase:function(t,e){t.commit("SET_DATABASE",e)},newDocument:function(t){t.commit("SET_DOCUMENT",{})}},getters:{listDatabases:function(t){return t.dbs_info},listViewItems:function(t){return t.view},getDocument:function(t){return t.document},getDatabase:function(t){return t.database}}});r["a"].config.productionTip=!1,new r["a"]({router:wt,store:Pt,render:function(t){return t(h)}}).$mount("#app")},"64a9":function(t,e,n){},"8a29":function(t,e,n){"use strict";var r=n("a4ae"),a=n.n(r);a.a},9796:function(t,e,n){},a4ae:function(t,e,n){},aea8:function(t,e,n){"use strict";var r=n("9796"),a=n.n(r);a.a}});
//# sourceMappingURL=app.3a09f203.js.map