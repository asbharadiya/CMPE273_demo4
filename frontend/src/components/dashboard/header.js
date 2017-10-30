import React, { Component } from 'react';
import {connect} from 'react-redux';
import * as actions from '../../actions';

class Header extends Component {

  logout(){
    this.props.logout();
  }

	render() {
    return (
    		<header>
    			<div className="topnav-container">
            <a onClick={()=>this.props.logout()}>Logout</a>
    			</div>
    		</header>
  	);
	}
}

function mapStateToProps(state) {
    return {};
}

function mapDispatchToProps(dispatch) {
    return {
        logout : () => dispatch(actions.logout())
    };
}

export default connect(mapStateToProps,mapDispatchToProps)(Header);
