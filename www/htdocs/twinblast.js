Ext.onReady(function(){


    // Pull out what's in the URL
    var vars = getUrlVars();

    var id = vars.id ? unescape(vars.id) : undefined;
    var qlist = vars.qlist ? unescape(vars.qlist) : undefined;
    // We need these three if we're going to use a single file
    var file = vars.file ? unescape(vars.file) : undefined;
    var left_suff = vars.leftsuff ? vars.leftsuff : '_bac';
    var right_suff = vars.rightsuff ? vars.rightsuff : '_human';

    // We'll need these 2 if we're using multiple files
    var left_file = vars.leftfile ? unescape(vars.leftfile) : undefined;
    var right_file = vars.rightfile ? unescape(vars.rightfile) : undefined;

    //If we have an ID and a file we'll start with the form collapsed
    var collapse_form = false;
    var single_file = false;

    if(id && file) {
        collapse_form = true;
        single_file = true;
    }
    if(id && left_file && right_file) {
        collapse_form = true;
    }
    var show_list = false;
    if(qlist) {
        show_list = true;
    }


    // Left Side
    var leftpanel = Ext.create('Ext.panel.Panel', ({
//        title: 'Bacterial',
 //       layout: 'fit',
        id: 'left_side',
        html: 'Nothing loaded yet',
        autoScroll: true,
        region: 'west',
        width: Ext.getBody().getViewSize().width/2,
//        flex: 1
    }));


    // Right Side
    var rightpanel = Ext.create('Ext.panel.Panel', ({
//        title: 'Human',
        html: 'Nothing loaded yet',
//        layout: 'fit',
        autoScroll: true,
        id: 'right_side',
        region: 'center',
        width: '50%',
        flex: 1
    }));


    var single_list_form = Ext.create('Ext.form.FieldContainer', ({
        defaultType: 'textfield',
        hidden: !single_file,
        items: [{
            fieldLabel: 'Blast List',
            name: 'blast',
            value: file
        },{
            fieldLabel: 'Left ID suffix',
            name: 'suff1',
            value: left_suff
        },{
            fieldLabel: 'Right ID suffix',
            name: 'suff2',
            value: right_suff
        }]
    }));

    var double_list_form = Ext.create('Ext.form.FieldContainer', ({
        defaultType: 'textfield',
        hidden: single_file,
        items: [{
            fieldLabel: 'Left BLAST output list',
            name: 'blast1',
            value: left_file
        },{
            fieldLabel: 'Right BLAST output list',
            name: 'blast2',
            value: right_file
        }]
    }));

    var type_radiogroup = Ext.create('Ext.form.RadioGroup', {
        defaultType: 'radio',
        defaults: {flex: 1},
        
        layout: 'hbox',
        items: [{
            boxLabel: '1 BLAST search, 2 ids',
            inputValue: '1',
            checked: single_file,
            name: 'num_lists',
            handler: function() {
                double_list_form.show();
                single_list_form.hide();
            }
        },{
            boxLabel: '2 BLAST searches, 1 id',
            inputValue: '2',
            checked: !single_file,
            name: 'num_lists',
            handler: function() {
                double_list_form.hide();
                single_list_form.show();
            }
        }]});
    var form = Ext.create('Ext.form.Panel', ({
//        layout: 'fit',
//        id: 'top',
        bodyPadding: 10,
        defaultType: 'textfield',
//        width: '50%',
        layout: 'hbox',
        defaults: {flex: 1},
        items: [
            {xtype: 'fieldset',
             title: 'Config',
             defaultType: 'textfield',
             items: [
                 {fieldLabel: 'List of queries (Optional)',
                  name: 'qlist',
                  value: qlist
                 },
                 {fieldLabel: 'ID (Optional)',
                  name: 'id',
                  value: id
                 },
             ]},
            {xtype: 'fieldset',
             title: 'BLAST lists',
             items: [
                 type_radiogroup,
                 single_list_form,
                 double_list_form]
           }]
    }));
    
    // Form    
    var toppanel =  Ext.create('Ext.panel.Panel', ({
//        layout: 'fit',
        //        id: 'top',
        frame: true,
        region: 'north',
        split: true,
        collapseMode: 'header',
        collapsed: collapse_form,
        collapsible: true,
        title: 'TwinBlast',
        defaultType: 'textfield',
        items: [form
        ],
        height: 250,
        buttonAlign: 'center',
        buttons: [{
            text: 'Reload',
            handler: function() {
                reloadPanels();
            }  
        }]
    }));

    Ext.define('links', {
        extend: 'Ext.data.Model',
        fields: [
            {name: 'name', type: 'string'},
        ]
    });
    // List
    var linkStore = Ext.create('Ext.data.Store', {
        storeId:'linkStore',
        //model: 'links',
        fields: ['name'],
        pageSize: 500,
        proxy: {
            type: 'ajax',
            url: '/cgi-bin/guiblast',
            actionMethods: {
                read: 'POST'
            },
            reader: {
                type: 'json',
                root: 'root'
            }
        },
        autoLoad: false,
    });
    var gridpanel = Ext.create('Ext.grid.Panel', ({
        store: linkStore,
        columns: [{header: 'link', dataIndex: 'name', flex: 1}],
        region: 'east',
        forcefit: true,
        width: 250,
        title: 'Query List',
        collapsed: !show_list,
        collapsible: true,
        dockedItems: [{
            xtype: 'pagingtoolbar',
            store: linkStore,   // same store GridPanel is using
            dock: 'bottom',
            displayInfo: true
        }]
    }));
    // update panel body on selection change
    gridpanel.getSelectionModel().on('selectionchange', function(sm, selectedRecord) {
        if(selectedRecord.length) {
            reloadPanels({id: selectedRecord[0].data.name});
        }
    });
    var vp = new Ext.Viewport({
        layout: 'border',
        autoScroll: true,
        defaults: {split: true},
//        items: [{
/*            defaults: {frame: true},
            region: 'center',
            title: 'foobar',
            height: 500,*/
        items: [toppanel,leftpanel,rightpanel,gridpanel],
/*            layout: 'hbox',
            align: 'stretchmax',
            pack: 'start'*/
//        }]
    });
    
    vp.doLayout();
    
    reloadPanels({});
    
    function getUrlVars() {
        var vars = {};
        var parts = parent.location.hash.replace(/[?&]+([^=&]+)=([^&#]*)/gi,
            function(m,key,value) {
                vars[key] = value;
            });
        return vars;
    } 

    function reloadPanels(newvals) {
        console.log(newvals);
        var vals = form.getValues();
        Ext.apply(vals,newvals);
        console.log(vals);
        if(vals.qlist) {
            linkStore.proxy.extraParams = {'qlist': qlist};
            linkStore.load();
        }
        if(vals.num_lists== "1" && vals.blast && vals.id) {
            reloadPanel(vals.blast,vals.id + vals.suff1,leftpanel);
            reloadPanel(vals.blast,vals.id + vals.suff2,rightpanel);
            setUrlVars({
                'leftsuff' : vals.suff1,
                'rightsuff' : vals.suff2,
                'id': vals.id,
                'file' : vals.blast
            });
        
        }else if(vals.num_lists == "2" && vals.blast1 && vals.blast2 && vals.id) {
            reloadPanel(vals.blast1,vals.id,leftpanel);
            reloadPanel(vals.blast2,vals.id,rightpanel);
            setUrlVars({
                'leftfile' : vals.blast1,
                'rightfile' : vals.blast2,
                'id': vals.id,
                'qlist' : vals.qlist
            });
        
        }
    
    }
    function reloadPanel(list,id,panel) {
        panel.setLoading({msg: 'Patience my friend...'});
        Ext.Ajax.request({
            url: '/cgi-bin/guiblast',
            params: {
                list: list,
                //list: '/local/projects/HLGT/driley/output_repository/ncbi-blastn/9357_default/ncbi-blastn.raw_lgt.list',
                id: id
            },
            success: function(response) {
                panel.update(response.responseText);
                panel.setLoading(false);
                // vp.doLayout();
            },
            failure: function(response) {
                Ext.Msg.alert('Error', 'Had a problem loading '+ id + 
                '.<br/>The server may be a bit overloaded. Give it another try.');
            }
        }); 
    }
    
    function setForm(obj) {
        

    }
    function setUrlVars(obj) {
        var url = Ext.urlEncode(obj);
        parent.location.hash = '?'+url;
    }
});
