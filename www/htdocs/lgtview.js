Ext.onReady(function(){

    var get_metadata = 0;
    Ext.regModel('gene',{
/*        defaults:{
            width: 120,
            sortable: true
        }*/
        fields: [
            {mapping: '_id',name: 'feat_type',type: 'string'},
            {name: 'count', type: 'int'}
        ]
    });
    var genestore = new Ext.data.Store({
        model: 'gene',
        autoLoad: false,
        proxy: {
            type: 'ajax',
            url: '/cgi-bin/lgtview/view.cgi',
            extraParams: {
                'criteria': 'hits.feat_type',
                'db': 'lgt8',
                'host': 'skimaro-lx.igs.umaryland.edu',
//                'cond': 'Escherichia',
//                'condfield': 'scientific'
            },
            reader: {
                type: 'json',
                root: 'retval'
            }
        }
    });



    Ext.regModel('bac',{
/*        defaults:{
            width: 120,
            sortable: true
        }*/
        fields: [
            {mapping: '_id',name: 'scientific',type: 'string'},
            {name: 'count', type: 'int'}
        ]
    }
    );

    Ext.regModel('hum',{
/*        defaults:{
            width: 120,
            sortable: true
        }*/
        fields: [
            {mapping: '_id',name: 'hu_ref',type: 'string'},
            {name: 'count', type: 'int'}
        ]
    }
    );    
    var bacstore = new Ext.data.Store({
        model: 'bac',
        autoLoad: false,
        listeners: {
            load: {fn: function() {
                bacstore.filterBy(
                    function(rec,id){
                        if(rec.data.scientific != "Other") {
                            return true;
                        }else { 
                            return false;
                        }
                    }); }}
        },
            
        proxy: {
            type: 'ajax',
            url: '/cgi-bin/lgtview/view.cgi',
            extraParams: {
                'criteria': 'hits.genus',
                'db': 'lgt8',
                'host': 'skimaro-lx.igs.umaryland.edu',
//                'cond': 'Escherichia',
//                'condfield': 'scientific'
            },
            reader: {
                type: 'json',
                root: 'retval'
            }
        }
    });

    Ext.regModel('hum',{
/*        defaults:{
            width: 120,
            sortable: true
        }*/
        fields: [
            {mapping: '_id',name: 'hu_ref',type: 'string'},
            {name: 'count', type: 'int'}
        ]
    }
    );    
    var humstore = new Ext.data.Store({
        model: 'hum',
        autoLoad: false,
        proxy: {
            type: 'ajax',
            url: '/cgi-bin/lgtview/view.cgi',
            extraParams: {
                'criteria': 'hu_ref',
                'db': 'lgt8',
                'host': 'skimaro-lx.igs.umaryland.edu'
            },
            reader: {
                type: 'json',
                root: 'retval'
            }
        }
    });

    Ext.regModel('sample',{
/*        defaults:{
            width: 120,
            sortable: true
        }*/
        fields: [
            {mapping: '_id',name: 'cancer_type',type: 'string'},
            {name: 'count', type: 'int'}
        ]
    }
    ); 
    var samplestore = new Ext.data.Store({
        model: 'sample',
        autoLoad: false,
        proxy: {
            type: 'ajax',
            url: '/cgi-bin/lgtview/view.cgi',
            extraParams: {
                'criteria': 'cancer_type',
                'db': 'lgt8',
                'host': 'skimaro-lx.igs.umaryland.edu'
            },
            reader: {
                type: 'json',
                root: 'retval'
            }
        }
    });
    
    Ext.regModel('filters',{
/*        defaults:{
            width: 120,
            sortable: true
        }*/
        fields: [
            {name: 'key',type: 'string'},
            {name: 'value', type: 'string'},
            {name: 'op', type: 'string'}
        ]
    }
    );
    var filterstore = new Ext.data.Store({
        model: 'filters',
        proxy: {
            type: 'memory',
            reader: {
                type: 'json',
                root: 'loads'
            }
        }
    });    
    var cellEditing = Ext.create('Ext.grid.plugin.CellEditing', {
        clicksToEdit: 1
    });
    var filtergrid = new Ext.grid.Panel({
        store: filterstore,
        forcefit: true,
//        width: '100%',
//        height: '100%',
        anchor: '100%, 100%',
        flex: 1,
        selModel: {
            selType: 'cellmodel'
        },
        plugins: [cellEditing],
        columns: [
            {text: 'Key', dataIndex: 'key', type: 'string',width: 80},
            {header: 'Op',
            dataIndex: 'op',
            width: 60,
            field: {
                xtype: 'combobox',
                typeAhead: true,
                triggerAction: 'all',
                selectOnTab: true,
                store: [
                    ['=','eq'],
                    ['!=','ne']
                ],
                lazyRender: true,
                listClass: 'x-combo-list-small'
            }},
            {text: 'value', dataIndex: 'value', type: 'string',width:80},

            {xtype: 'actioncolumn',
                width: 20,
                items: [{
                    icon   : 'delete.gif',  // Use a URL in the icon config
                    tooltip: 'Remove filter',
                    handler: function(grid, rowIndex, colIndex) {
                        var rec = filterstore.getAt(rowIndex);
//                        delete allfilters[rec.data.key]
                        filterstore.remove(rec);
                        loadData();
                    }
                }]
            }
        ]
    });

    var cov_field = new Ext.form.field.Text({
        fieldLabel: 'min coverage',
        //value: ,
        name: 'cov'
    });
    var blast_gen = new Ext.form.field.Text({
        fieldLabel: 'BLAST genus',
        name: 'blast_gen'
    });
    var blast_valid = new Ext.form.field.Checkbox({
        fieldLabel: 'Valid BLAST',
        name: 'blast_val'
    });
    var filterform = new Ext.form.Panel({
//        height: '100%',
        width: '100%',
        frame: true,
        items: [cov_field,blast_gen,blast_valid]
    });

    Ext.regModel('reads',{
/*        defaults:{
            width: 120,
            sortable: true
        }*/
        fields: [
            {name: 'read',type: 'string'},
            {name: 'twinblast', type: 'string'},
        ]
    }
    );
    var readstore = new Ext.data.Store({
        model: 'reads',
        pageSize: 100,
        proxy: {
            type: 'ajax',
            url: '/cgi-bin/lgtview/view.cgi',
            extraParams: {
                'db': 'lgt8',
                'host': 'skimaro-lx.igs.umaryland.edu'
            },
            reader: {
                type: 'json',
                root: 'retval'
            }
        }
    });
    
    var readgrid = new Ext.grid.Panel({
        store: readstore,
        title: 'Reads',
        region: 'east',
        forcefit: true,
        width: 300,
        split: true,
//        flex: 1,
        columns: [
            {text: 'read', dataIndex: 'read', type: 'string',width: 80},
            {text: 'twinview', dataIndex: 'read', type: 'string',width:80,
            renderer: function(value,p,record) {return '<a target=_blank href=http://driley-lx.igs.umaryland.edu:8080/lgtview/twinblast.html?id='+value+'>'+value+'</a>';}},
        ],
        // paging bar on the bottom
        bbar: Ext.create('Ext.PagingToolbar', {
            store: readstore,
            displayInfo: true,
            displayMsg: 'Displaying reads {0} - {1} of {2}',
            emptyMsg: "No reads to display"
        }),
    });

    var vp = new Ext.Viewport({
        items: [
            {xtype: 'panel',
            layout: 'fit',
            region: 'center'
/*            tbar: new Ext.Toolbar({
                items: ['Seconds between reload:',
                        reload_combo,
                        {xtype: 'button',
                         text: 'Stop AutoReload',
                         handler: function() {
                             if(this.text == 'Stop AutoReload') {
                                 runner.stop(reload_task);
                                 this.setText('Start AutoReload'); 
                             }
                             else {
                                 runner.start(reload_task);
                                 this.setText('Stop AutoReload'); 
                             }
                         }
                        }]
                
            })*/
            },readgrid,
            {layout: 'fit',
            region: 'west',
            title: 'Filters',
            buttons: [{text: 'reload',handler: function() { loadData()}}],
            split: true,
            items: [{layout: 'anchor',
//                    align : 'stretch',
//                    pack  : 'start',
                    items: [
                    filterform,
                    filtergrid]
                    }],
            width: 300}],
        layout: 'border',

    });
    var genechart = new Ext.chart.Chart({
        animate: true,
        store: genestore,
        shadow: false,
        legend: {
            position: 'right'
        },
        //insetPadding: 60,
        theme: 'Base:gradients',
        series: [{
            type: 'pie',
            field: 'count',
//            display: 'none',
            listeners: {
                'itemmouseup': function(item) {
                    loadData(bacstore,{'hits.feat_type': item.storeItem.data.feat_type});
                }
            },
            tips: {
                width: 290,
                renderer: function(storeItem, item) {
                    var title = 'Unknown';
                    if(storeItem.get("feat_type")) {
                        title = storeItem.get("feat_type");
                    }
                    this.setTitle(title+'<br/>'+storeItem.get('count')+' reads');
                }
            },
            highlight: {
                segment: {
                    margin:20
                }
            },
            /*label: {
                field: 'feat_type',
                display: 'rotate',
                contrast: true
            }*/
        }]
    });
    var bacbar = new Ext.chart.Chart({
        animate: false,
        store: bacstore,
        shadow: false,
        height: 2000,
        width: 475,
        //insetPadding: 10,
        axes: [{
            type: 'Numeric',
            position: 'top',
            fields: ['count'],
            label: {
                renderer: Ext.util.Format.numberRenderer('0,0')
            },
            title: 'Number of Reads',
            grid: true,
            minimum: 0,
            //maximum: 150000 
        }, {
            type: 'Category',
            position: 'left',
            fields: ['scientific'],
            //display: false,
            //label: {display: false},
            title: 'Bacteria'
        }],
            series: [{
                type: 'bar',
                axis: 'top',
                //highlight: true,
                tips: {
                  trackMouse: true,
                  width: 220,
                  height: 30,
                  renderer: function(storeItem, item) {
                    this.setTitle(storeItem.get('scientific') + ': ' + storeItem.get('count') + ' reads');
                  }
                },
                xField: 'scientific',
                yField: ['count']
            }]

    });
    var bacchart = new Ext.chart.Chart({
        animate: true,
        store: bacstore,
        shadow: false,
        legend: {
            position: 'right'
        },
        //insetPadding: 60,
        theme: 'Base:gradients',
        series: [{
            type: 'pie',
            field: 'count',
//            display: 'none',
            listeners: {
                'itemmouseup': function(item) {
                    loadData(bacstore,{'hits.genus': item.storeItem.data.scientific});
                }
            },
            tips: {
                width: 250,
                renderer: function(storeItem, item) {
                    var title = 'Unknown';
                    if(storeItem.get("scientific")) {
                        title = storeItem.get("scientific");
                    }
                    this.setTitle(title+'<br/>'+storeItem.get('count')+' reads');
                }
            },
            highlight: {
                segment: {
                    margin:20
                }
            },
            label: {
                field: 'scientific',
                display: 'rotate',
                contrast: true
            }
        }]
    });
    var humchart = new Ext.chart.Chart({
        animate: true,
        store: humstore,
        shadow: false,
        //legend: {
        //    position: 'right'
        //},
        //insetPadding: 60,
        theme: 'Base:gradients',
        series: [{
            showInLegend: true,
            type: 'pie',
            field: 'count',
            display: 'none',
            listeners: {
                'itemmouseup': function(item) {
                    loadData(humstore,{'hu_ref': item.storeItem.data.hu_ref});
                }
            },
            tips: {
                width: 170,
                renderer: function(storeItem, item) {
                    var title = 'Unknown';
                    if(storeItem.get("hu_ref")) {
                        title = storeItem.get("hu_ref");
                    }
                    this.setTitle(title+'<br/>'+storeItem.get('count')+' reads');
                }
            },
            highlight: {
                segment: {
                    margin:20
                }
            },
            label: {
                field: 'hu_ref',
         //       display: 'rotate',
         //       contrast: true
            }
        }]
    });
    var samplechart = new Ext.chart.Chart({
        animate: true,
        store: samplestore,
        shadow: false,
//        legend: {
//            position: 'right'
//        },
        //insetPadding: 60,
        theme: 'Base:gradients',
        series: [{
            type: 'pie',
            field: 'count',
            display: 'none',
            listeners: {
                'itemmouseup': function(item) {
                    loadData(samplestore,{'cancer_type': item.storeItem.data.cancer_type});
                }
            },
            tips: {
                width: 170,
                renderer: function(storeItem, item) {
                    this.setTitle(storeItem.get("cancer_type")+'<br/>'+storeItem.get('count')+' reads');
                }
            },
            highlight: {
                segment: {
                    margin:20
                }
            },
            label: {
                field: 'cancer_type',
                display: 'rotate',
                contrast: true
            }
        }]
    });
    vp.doLayout();
    var genewin = new Ext.Window({
        title: 'Gene Mappings',
        layout: 'fit',
        x: 150,
        y: 10,
        width: 500,
        height: 400,
        loader: {
            loadMask: false
        },
        items: genechart
    });
    genewin.show();
    var bacwin = new Ext.Window({
        title: 'Bacterial Mappings',
        //layout: 'fit',
        x: 150,
        y: 10,
        width: 500,
        height: 400,
        autoScroll: true,
        loader: {
            loadMask: false
        },
//        items: bacchart
        items: bacbar
    });
    bacwin.show();

    var humwin = new Ext.Window({
        title: 'Human Mappings',
        layout: 'fit',
        x: 700,
        y: 10,
        width: 500,
        height: 400,
        loader: {
            loadMask: false
        },
        items: humchart
    });
    humwin.show()
    
    var samplewin = new Ext.Window({
        title: 'Sample Distribution',
        layout: 'fit',
        height: 400,
        width: 500,
        x: 400,
        y: 450,
        items: samplechart
    });
    samplewin.show();

    var allStores = [bacstore,humstore,samplestore,readstore,genestore];
    var allfilters = {};
    loadData();

    function loadData(caller,cond) {
        appendFilter(cond);
//        var caller_id = caller.model.modelName;
        allfilters = {};
        filterstore.each(function(rec) {
            if(rec.data.op == '=') {
                allfilters[rec.data.key] = rec.data.value;
            }
            else if(rec.data.op == '!=') {
                allfilters[rec.data.key] = {'$ne': rec.data.value};
            }
        });
        if(cov_field.getValue() != '') {
            allfilters['hu_cov'] = {'$gt' : cov_field.getValue()*1};
        }
        if(blast_gen.getValue() != '') {
            allfilters['bac_blast'] = {'$regex': blast_gen.getValue()};
        }
        else if(blast_valid.getValue()) {
            allfilters['bac_blast'] = {'$ne': null};
        }
        Ext.each(allStores, function(store) {
        
            // Monsta hack here. Should do this in a listener on the store!!
            if(store.model.modelName =='bac') {
                if(allfilters['hits.genus'] != null) {
                    store.getProxy().extraParams.criteria = 'hits.scientific';
                }
                else {
                    store.getProxy().extraParams.criteria = 'hits.genus';
                }
            }
            // Monsta hack here. Should do this in a listener on the store!!
            if(store.model.modelName =='gene') {
                if(allfilters['hits.feat_type'] != null) {
                    store.getProxy().extraParams.criteria = 'hits.feat_product';
                }
                else {
                    store.getProxy().extraParams.criteria = 'hits.feat_type';
                }
            }
//            if(store.model.modelName != caller_id) {
                Ext.apply(store.getProxy().extraParams,
                    {cond: Ext.encode(allfilters),
                });
                store.load();
//            }
        });
    }
    

    
    function appendFilter(filter) { 
        for(i in filter) if (filter.hasOwnProperty(i)) {
            if(filterstore.findRecord('key',i)) {
                var rec = filterstore.findRecord('key',i);
                rec.set('value',filter[i]);
                rec.set('op', '=');
//                rec.data.op = '=';
 //               allfilters[i] = filter[i];
            }
            else {
//                allfilters[i] = filter[i];
                filterstore.add({
                    'key': i,
                    'op': '=',
                    'value': filter[i]
                });
            }
        }
    }
});


