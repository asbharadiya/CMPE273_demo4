import React, { Component } from 'react';

class ItemRow extends Component {

	render() {
		return (
      <div className="item-row" id="itemRow">
    		<div className="item-icon">
          <i className="fa fa-file fa-3x" aria-hidden="true"></i>
    		</div>
    		<div className="item-content">
    			<p className="item-title">{this.props.item.name}</p>
    		</div>
        <div className="item-options">
          
        </div>
	    </div>
  	);
	}
}

export default ItemRow;
