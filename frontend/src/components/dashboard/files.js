import React, { Component } from 'react';
import {withRouter} from 'react-router-dom';
import {connect} from 'react-redux';
import ItemRow from './itemrow';
import RightContent from './rightcontent';
import * as actions from '../../actions/asset';

class Files extends Component {

	constructor(props){
		super(props);
    this.loadPage = this.loadPage.bind(this);
	}

	componentDidMount(){
		this.loadPage(this.props);
	}

  componentWillReceiveProps(nextProps){
    if(this.props.uploadFileSuccess !== nextProps.uploadFileSuccess && nextProps.uploadFileSuccess){
      this.loadPage(nextProps);
    }
  }

  loadPage(props){
    this.props.getAssets();
  }

	render() {
    return (
    		<div className="inner-page-content has-right-content">
    			<div className="filespage">
    				<div className="page-header">
	    			  <h4>Files</h4>
	    		  </div>
    				{
              this.props.assets.length > 0 ? (
          			this.props.assets.map(function(item,index) {
                  return (
                    	<ItemRow key={index} item={item}/>
                  );
                })
  	          ) : (
                <p className="empty-page-msg">This folder is empty</p>
              )
            }
      		</div>
      		<RightContent pagetype="files"/>
    		</div>
  	);
	}
}

function mapStateToProps(state) {
    return {
      assets:state.assets,
      uploadFileSuccess:state.uploadFileSuccess
    };
}

function mapDispatchToProps(dispatch) {
    return {
        getAssets : () => dispatch(actions.getAssets())
    };
}

export default withRouter(connect(mapStateToProps,mapDispatchToProps)(props => <Files {...props}/>));
